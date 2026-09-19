# The platform's one image: every service on the live stack runs `verdict`
# with a different command (deploy/live/compose.yml). Built from the
# repository root:
#
#   docker build -f deploy/image/Dockerfile -t verdict .
#
# deploy/push-image.sh builds it, tags it with the commit, and pushes it to
# the stack's ECR repository, whose tags are immutable (ADR 14).

# The same minor as the build machine and CI (pyproject.toml requires 3.13).
FROM python:3.13-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY verdict ./verdict
# The runtime dependencies only: the dev extras (pytest, mypy, stubs) never
# reach the instance.
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.13-slim
# A fixed, unprivileged user. The boot script gives it the history directory
# on the data volume by this uid.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin verdict
COPY --from=build /install /usr/local
USER 10001
ENTRYPOINT ["verdict"]
CMD ["--help"]
