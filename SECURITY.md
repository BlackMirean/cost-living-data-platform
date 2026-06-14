# Security Policy

## Supported Scope

Security reporting covers this public repository, deployment manifests and scripts. It does not cover third-party services or datasets.

## Reporting

Please report sensitive issues privately to the repository owner. Do not open a public issue containing credentials, tokens, kubeconfigs, private URLs, screenshots with secrets, or raw personal data.

## Secret Handling

- Runtime secrets belong in Kubernetes Secrets, local `.env` files or platform secret stores.
- Example manifests must contain non-secret sample values only.
- Rotate any token that may have been committed, logged or shared.
- Run `make public-check` before publishing a release.

## Dependency Updates

Dependency updates should keep tests passing and should include a short note if they change API behavior, Elasticsearch mappings or deployment requirements.
