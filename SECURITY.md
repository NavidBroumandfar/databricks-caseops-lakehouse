# Security Policy

## Supported Scope

This is a public, non-production reference project. It intentionally excludes
production Databricks credentials, workspace URLs, account IDs, tokens, customer
data, private runtime evidence, and downstream Bedrock runtime secrets.

Security-sensitive concerns for this repository include:

- accidental exposure of secrets or workspace identifiers
- committed customer or proprietary data
- unsafe handling of generated local outputs
- schema or contract changes that could silently pass invalid Gold records to
  downstream consumers
- documentation that overstates production deployment or live Bedrock behavior

## Reporting a Vulnerability

Please open a private report through GitHub's security reporting flow if
available, or contact the repository owner directly through the profile links in
the README.

Do not include real secrets, tokens, private workspace URLs, customer records, or
other sensitive data in public issues or pull requests. Redact examples before
sharing them.

## Disclosure Expectations

This repository is maintained as a portfolio-safe reference implementation. A
reasonable response will prioritize:

1. Removing exposed sensitive material
2. Preserving the upstream Databricks preparation boundary
3. Updating tests, validators, and docs when contract behavior changes
4. Keeping public claims aligned with validated non-production evidence
