"""Opening a pull request from the instance, with nothing but GitHub's REST API (ADR 28).

The live retraining job and the promotion gate end at a pull request a person
merges (ADR 24, ADR 11). They run on the instance, where there is no clone of
the repository and no reason to keep one, so a pull request is made the way
the REST API allows without git: the files become blobs, the blobs a tree on
top of `main`'s, the tree a commit, the commit a branch, and the branch a pull
request. Nothing is pushed to `main` and nothing is merged.

The token is a fine-grained one scoped to this repository, allowed to write
contents and pull requests and nothing else. It reaches the job through an
environment variable the boot script fills from SSM; it is never logged, and
it appears in no message this module raises.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Final

API: Final = "https://api.github.com"
REPOSITORY: Final = "Peter-A-P/realtime-risk-platform"

Transport = Callable[[str, str, dict[str, Any] | None], dict[str, Any]]
"""(method, path under the API, JSON body or None) to the parsed response."""


class GitHubError(RuntimeError):
    """Raised when GitHub refuses a request."""


@dataclass(frozen=True, slots=True)
class FileChange:
    """One file a pull request adds or replaces.

    Attributes:
        path: Where in the repository.
        content: Its bytes.
    """

    path: str
    content: bytes


def urllib_transport(token: str, *, timeout_seconds: float = 60.0) -> Transport:
    """The transport that talks to GitHub.

    Args:
        token: The fine-grained token.
        timeout_seconds: Per request.

    Returns:
        The transport.
    """

    def send(method: str, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{API}{path}",
            data=None if body is None else json.dumps(body).encode("utf-8"),
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "verdict-live",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                parsed: dict[str, Any] = json.loads(response.read() or b"{}")
                return parsed
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", "replace")[:300]
            msg = f"GitHub refused {method} {path}: {error.code} {detail}"
            raise GitHubError(msg) from None

    return send


class GitHub:
    """Just enough of the API to open a pull request."""

    def __init__(self, transport: Transport, *, repository: str = REPOSITORY) -> None:
        """Talk to one repository.

        Args:
            transport: How requests are sent.
            repository: `owner/name`.
        """
        self.transport = transport
        self.repository = repository

    def _repo(self, path: str) -> str:
        return f"/repos/{self.repository}{path}"

    def read_file(self, path: str, *, ref: str = "main") -> bytes:
        """A file's bytes as they are on a branch.

        Args:
            path: The file.
            ref: The branch.

        Returns:
            Its bytes.
        """
        item = self.transport("GET", self._repo(f"/contents/{path}?ref={ref}"), None)
        return base64.b64decode(item["content"])

    def open_pull_request(
        self,
        *,
        branch: str,
        title: str,
        body: str,
        message: str,
        files: Sequence[FileChange],
        base: str = "main",
    ) -> str:
        """Commit files on a new branch from `base` and open a pull request for it.

        Args:
            branch: The new branch's name; it must not exist.
            title: The pull request's title.
            body: Its description, markdown.
            message: The commit message.
            files: What the commit adds or replaces.
            base: The branch it is based on and asks to merge into.

        Returns:
            The pull request's web address.
        """
        head = self.transport("GET", self._repo(f"/git/ref/heads/{base}"), None)
        parent = head["object"]["sha"]
        commit = self.transport("GET", self._repo(f"/git/commits/{parent}"), None)
        entries = []
        for change in files:
            blob = self.transport(
                "POST",
                self._repo("/git/blobs"),
                {"content": base64.b64encode(change.content).decode("ascii"), "encoding": "base64"},
            )
            entries.append(
                {"path": change.path, "mode": "100644", "type": "blob", "sha": blob["sha"]}
            )
        tree = self.transport(
            "POST",
            self._repo("/git/trees"),
            {"base_tree": commit["tree"]["sha"], "tree": entries},
        )
        new = self.transport(
            "POST",
            self._repo("/git/commits"),
            {"message": message, "tree": tree["sha"], "parents": [parent]},
        )
        self.transport(
            "POST", self._repo("/git/refs"), {"ref": f"refs/heads/{branch}", "sha": new["sha"]}
        )
        pull = self.transport(
            "POST",
            self._repo("/pulls"),
            {"title": title, "head": branch, "base": base, "body": body},
        )
        address: str = pull["html_url"]
        return address
