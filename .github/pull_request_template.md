## Summary

<!-- What changes and why, in a few lines. Link the roadmap phase or issue. -->

## Checks run

- [ ] `make ci` (lint, types, unit tests with coverage, compose, hadolint, shellcheck, actionlint, gitleaks)
- [ ] `make up && make smoke && make test-integration` (when the stack, compose or the app image changes)
- [ ] Docs updated (README, `docs/`, ADR) if behaviour or architecture changed

## Risks

<!-- What could break, for whom, and how to roll back. "None" is a valid answer if you say why. -->
