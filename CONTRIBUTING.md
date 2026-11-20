# Contributing

## Workflow

1. One branch and one pull request per feature or fix, from an up-to-date `main`
   (`git switch -c feat/<topic> origin/main`).
2. Run `make ci` before pushing: it runs the same checks as the CI Quality and Repository jobs.
   When the change touches the stack, the compose files or the app image, also run
   `make up && make smoke && make test-integration`.
3. Open the pull request with the template filled in (summary, checks run, risks). `main` only
   receives reviewed pull requests with green CI.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`,
`refactor:`, `build:`, `ci:`, `chore:`, with an optional scope (`feat(catalog): ...`). The subject
says what changes in the imperative; the body says why.

## Setup

```bash
make sync                      # Python 3.12 environment from uv.lock
uv run pre-commit install      # ruff and gitleaks on every commit
make env && make up            # local stack
```

## Rules of thumb

- Branch coverage stays at or above 90 %, measured over the whole `ibergrid` package. Code is
  never excluded from coverage to reach it; write the test.
- Tests exercise behaviour against real local servers and sockets rather than mocks of our own
  functions. Integration tests carry the `integration` marker and need `make up`.
- Third-party images are pinned by tag and digest, GitHub Actions by commit SHA.
- No secrets in the repository: `.env` is generated locally (`make env`) and git-ignored, and
  `.env.example` holds placeholders only.
- Architecture changes come with an ADR in `docs/adr/`.
