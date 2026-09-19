"""The challenger: an FT-Transformer, the honest neural baseline for tabular data.

The plan's challenger (`PLAN.md` section 5, week 5) is the FT-Transformer of
Gorishniy et al. (2021): every input becomes a token through its own linear
embedding, a learned [CLS] token joins them, a few pre-norm Transformer blocks
mix them, and a head reads the [CLS] token. It is small here on purpose
(two blocks, 32-dimensional tokens), trained on the CPU, and it gets no more
tuning than the champion did: the same fixed-before-testing rule (ADR 1).

It sees exactly the champion's inputs (`MODEL_INPUTS`), and the only
preprocessing is inside the exported graph, so the scorer passes it the same
vector it passes the champion:

- a feature with no history carries the `NO_EVENTS` sentinel; the graph turns
  it into a flag, which gets its own learned embedding, and a zero value;
- every other value is `log1p` of itself (counts, amounts and seconds are all
  heavy-tailed and non-negative), then standardised with the training rows'
  weighted mean and spread.

Training uses each row's weight in the loss, early-stops on weighted PR-AUC
over the latest part of the training rows, and is seeded. The model is
exported to ONNX with a `probabilities` output of two columns, so it runs
behind `OnnxModel` exactly as the champion does, in shadow (ADR 11).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import numpy as np
import numpy.typing as npt

from verdict.models.dataset import TrainingSet
from verdict.models.inputs import MODEL_INPUTS
from verdict.models.promote import average_precision
from verdict.store.features import NO_EVENTS

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    import torch

TOKEN_DIM: Final = 32
BLOCKS: Final = 2
HEADS: Final = 4
DROPOUT: Final = 0.1
BATCH: Final = 2_048
LEARNING_RATE: Final = 1e-3
MAX_EPOCHS: Final = 12
PATIENCE: Final = 2
VALIDATION_SHARE: Final = 0.15
PARITY_TOLERANCE: Final = 1e-4


def _module_class() -> type[Any]:
    """The network, defined where torch is imported (it is an optional extra)."""
    import torch
    from torch import nn

    class FTTransformer(nn.Module):
        """Feature tokenizer, [CLS], pre-norm Transformer blocks, linear head."""

        def __init__(self, mean: npt.NDArray[np.float64], std: npt.NDArray[np.float64]) -> None:
            super().__init__()
            width = len(MODEL_INPUTS)
            self.register_buffer("mean", torch.tensor(mean, dtype=torch.float32))
            self.register_buffer("std", torch.tensor(std, dtype=torch.float32))
            self.weight = nn.Parameter(torch.randn(width, TOKEN_DIM) * 0.02)
            self.bias = nn.Parameter(torch.zeros(width, TOKEN_DIM))
            self.absent = nn.Parameter(torch.zeros(width, TOKEN_DIM))
            self.cls = nn.Parameter(torch.randn(1, 1, TOKEN_DIM) * 0.02)
            layer = nn.TransformerEncoderLayer(
                d_model=TOKEN_DIM,
                nhead=HEADS,
                dim_feedforward=TOKEN_DIM * 2,
                dropout=DROPOUT,
                batch_first=True,
                norm_first=True,
            )
            self.blocks = nn.TransformerEncoder(
                layer, num_layers=BLOCKS, enable_nested_tensor=False
            )
            self.norm = nn.LayerNorm(TOKEN_DIM)
            self.head = nn.Linear(TOKEN_DIM, 1)

        def forward(self, inputs: torch.Tensor) -> torch.Tensor:
            absent = (inputs == NO_EVENTS).to(inputs.dtype)
            value = torch.log1p(torch.clamp(inputs, min=0.0)) * (1.0 - absent)
            value = (value - self.get_buffer("mean")) / self.get_buffer("std")
            tokens = (
                value.unsqueeze(-1) * self.weight + self.bias + absent.unsqueeze(-1) * self.absent
            )
            cls = self.cls.expand(inputs.shape[0], -1, -1)
            mixed = self.blocks(torch.cat([cls, tokens], dim=1))
            logit = self.head(self.norm(mixed[:, 0, :])).squeeze(-1)
            fraud = torch.sigmoid(logit)
            return torch.stack([1.0 - fraud, fraud], dim=1)

    return FTTransformer


def _standardisation(
    inputs: npt.NDArray[np.float64], weights: npt.NDArray[np.float64]
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """Weighted mean and spread of each input after the graph's own transform."""
    absent = inputs == NO_EVENTS
    value = np.where(absent, 0.0, np.log1p(np.clip(inputs, 0.0, None)))
    w = weights[:, None]
    mean = (value * w).sum(axis=0) / w.sum()
    var = ((value - mean) ** 2 * w).sum(axis=0) / w.sum()
    return mean, np.sqrt(var) + 1e-6


@dataclass(slots=True)
class FittedChallenger:
    """A fitted challenger and how it was fitted.

    Attributes:
        module: The network, in evaluation mode.
        epochs: Epochs run.
        best_epoch: The epoch whose weights were kept.
        validation_pr_auc: Weighted PR-AUC on the early-stopping rows, per epoch.
        seconds: Wall time.
        settings: The fixed settings.
    """

    module: Any
    epochs: int = 0
    best_epoch: int = 0
    validation_pr_auc: list[float] = field(default_factory=list)
    seconds: float = 0.0
    settings: dict[str, Any] = field(default_factory=dict)


def fit_challenger(
    training: TrainingSet, *, seed: int = 20270405, threads: int = 6
) -> FittedChallenger:
    """Fit the FT-Transformer, early-stopping on the latest training rows.

    Args:
        training: The training set, rows in event-time order.
        seed: For initialisation, shuffling and dropout.
        threads: Torch threads.

    Returns:
        The fitted challenger.
    """
    import torch

    torch.manual_seed(seed)
    torch.set_num_threads(threads)
    rng = np.random.default_rng(seed)
    rows = len(training.labels)
    split = int(rows * (1 - VALIDATION_SHARE))
    x_fit, x_val = training.inputs[:split], training.inputs[split:]
    y_fit = training.labels[:split].astype(np.float32)
    w_fit, w_val = training.weights[:split], training.weights[split:]
    mean, std = _standardisation(x_fit, w_fit)
    module = _module_class()(mean, std)
    optimiser = torch.optim.AdamW(module.parameters(), lr=LEARNING_RATE, weight_decay=1e-5)
    x_fit_t = torch.tensor(x_fit, dtype=torch.float32)
    y_fit_t = torch.tensor(y_fit)
    w_fit_t = torch.tensor(w_fit / w_fit.mean(), dtype=torch.float32)
    x_val_t = torch.tensor(x_val, dtype=torch.float32)

    fitted = FittedChallenger(
        module=module,
        settings={
            "token_dim": TOKEN_DIM,
            "blocks": BLOCKS,
            "heads": HEADS,
            "dropout": DROPOUT,
            "batch": BATCH,
            "learning_rate": LEARNING_RATE,
            "max_epochs": MAX_EPOCHS,
            "patience": PATIENCE,
            "seed": seed,
        },
    )
    best_state: dict[str, Any] | None = None
    best = -1.0
    started = time.perf_counter()
    for epoch in range(MAX_EPOCHS):
        module.train()
        order = torch.tensor(rng.permutation(split))
        for start in range(0, split, BATCH):
            batch = order[start : start + BATCH]
            optimiser.zero_grad()
            fraud = module(x_fit_t[batch])[:, 1].clamp(1e-7, 1 - 1e-7)
            loss = torch.nn.functional.binary_cross_entropy(
                fraud, y_fit_t[batch], weight=w_fit_t[batch]
            )
            loss.backward()  # type: ignore[no-untyped-call]
            optimiser.step()
        scores = _predict(module, x_val_t)
        ap = average_precision(training.labels[split:], scores, w_val)
        fitted.validation_pr_auc.append(ap)
        fitted.epochs = epoch + 1
        if ap > best:
            best, fitted.best_epoch = ap, epoch + 1
            best_state = {k: v.detach().clone() for k, v in module.state_dict().items()}
        elif epoch + 1 - fitted.best_epoch >= PATIENCE:
            break
    if best_state is not None:
        module.load_state_dict(best_state)
    module.eval()
    fitted.seconds = time.perf_counter() - started
    return fitted


def _predict(module: Any, inputs: torch.Tensor) -> npt.NDArray[np.float64]:  # noqa: ANN401
    import torch

    module.eval()
    out: list[npt.NDArray[np.float64]] = []
    with torch.no_grad():
        for start in range(0, inputs.shape[0], 16_384):
            out.append(module(inputs[start : start + 16_384])[:, 1].numpy().astype(np.float64))
    return np.concatenate(out) if out else np.empty(0)


def challenger_scores(
    fitted: FittedChallenger, inputs: npt.NDArray[np.float64]
) -> npt.NDArray[np.float64]:
    """The challenger's fraud probabilities.

    Args:
        fitted: The model.
        inputs: Rows in `MODEL_INPUTS` order.

    Returns:
        One probability per row.
    """
    import torch

    return _predict(fitted.module, torch.tensor(inputs, dtype=torch.float32))


def export_challenger(
    fitted: FittedChallenger, path: Path, check_rows: npt.NDArray[np.float64]
) -> Path:
    """Write the challenger as ONNX, having checked it scores as torch does.

    Args:
        fitted: The model.
        path: Where to write it.
        check_rows: Rows to compare the two on.

    Returns:
        The path written.

    Raises:
        RuntimeError: If any score differs by more than `PARITY_TOLERANCE`.
    """
    import torch

    from verdict.scoring.onnx_model import OnnxModel

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    example = torch.tensor(check_rows[:2], dtype=torch.float32)
    torch.onnx.export(
        fitted.module,
        (example,),
        str(temporary),
        input_names=["inputs"],
        output_names=["probabilities"],
        dynamic_axes={"inputs": {0: "rows"}, "probabilities": {0: "rows"}},
        opset_version=17,
        dynamo=False,
    )
    exported = OnnxModel(temporary, prefix="challenger").score_matrix(check_rows)
    expected = challenger_scores(fitted, check_rows)
    worst = float(np.max(np.abs(exported - expected))) if len(expected) else 0.0
    if worst > PARITY_TOLERANCE:
        temporary.unlink()
        msg = f"ONNX scores differ from torch's by up to {worst:.2e}"
        raise RuntimeError(msg)
    temporary.replace(path)
    return path
