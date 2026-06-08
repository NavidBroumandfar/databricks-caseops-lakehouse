# Contributing

Thanks for taking a look at Databricks CaseOps Lakehouse.

This repository is a governed upstream document intelligence and handoff layer:
raw document in, structured AI-ready Gold record out. Keep changes inside that
boundary.

## Local Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

Equivalent Make targets:

```bash
make install-dev
make test
```

## Contribution Guidelines

- Preserve local-safe behavior without Databricks credentials.
- Keep examples public, synthetic, or redacted.
- Do not commit generated `output/` artifacts, virtualenv files, secrets,
  workspace URLs, account IDs, tokens, or customer data.
- Keep the upstream/downstream boundary explicit. Retrieval, RAG, Bedrock
  runtime logic, agent reasoning, escalation, and case-support workflows belong
  in the downstream Bedrock CaseOps project.
- When changing schemas, update validators, fixtures, tests, and documentation
  together.
- Prefer focused, contract-preserving changes over broad rewrites.

## Verification

Run the full local test suite before opening a pull request:

```bash
.venv/bin/python -m pytest -q
```

Docs-only changes do not require new tests, but the changed documentation should
be checked for scope drift and production overclaims.
