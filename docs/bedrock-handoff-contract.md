# Bedrock Handoff Contract — Gold → Bedrock CaseOps Interface

> **Contract Phase**: B-0 — Bedrock Handoff Contract Preparation
> **Status**: Established. This is the single authoritative contract artifact for the Gold layer handoff.
> **This document does not describe live integration.** Live integration is Phase B proper.
> Authoritative technical design is in [`ARCHITECTURE.md`](../ARCHITECTURE.md).
> Authoritative scope is in [`PROJECT_SPEC.md`](../PROJECT_SPEC.md).

---

## 1. Purpose

This contract defines the explicit interface boundary between:

- **Databricks CaseOps Lakehouse** — the governed upstream document intelligence and AI-ready asset preparation layer (this repo)
- **Bedrock CaseOps** — the downstream retrieval, reasoning, escalation, and case-support workflow layer

The contract exists to:

1. Make the Gold `export_payload` the explicit, versioned interface between the two systems
2. Define precisely what this repo guarantees to produce and what Bedrock CaseOps is expected to consume
3. Allow Bedrock CaseOps implementation to proceed without ambiguity about upstream data shape
4. Establish a stable, honest boundary that explicitly acknowledges current implementation limitations

B-0 is a **contract-hardening phase**. Its purpose is to define the handoff clearly enough that live integration work (Phase B proper) can begin without contract ambiguity. No AWS credentials, Bedrock SDK code, S3 plumbing, vector index configuration, or live integration logic is delivered here.

---

## 2. Ownership Boundary

### What Databricks CaseOps Lakehouse Guarantees (This Repo)

| Responsibility | Guarantee |
|---|---|
| Document ingestion | Raw files are ingested into Unity Catalog Volumes with a stable, immutable `document_id` assigned at ingest time |
| Parsing | `ai_parse_document` is applied; Bronze records carry parse status, full extracted text, and provenance metadata |
| Structured extraction | `ai_extract` produces schema-validated Silver records with domain-specific structured fields |
| Classification and routing | `ai_classify` produces document type labels and routing labels per the defined closed taxonomy |
| Export payload construction | Gold records carry a fully-structured `export_payload` for all `export_ready = true` records |
| Export payload materialization | One JSON file per `export_ready` record is written to a deterministic Volume path |
| Lineage completeness | Every Gold record is traceable to its Silver extraction, Bronze parse, and source file via `document_id` |
| Quarantine governance | Records failing quality thresholds are marked `export_ready = false` and routed to `quarantine` — never silently passed through |
| Schema versioning | Every record and payload carries `schema_version`; breaking changes increment the major version |
| Evaluation coverage | Evaluation passes are available for all four quality dimensions (Bronze, Silver, Gold, Traceability) |

### What Bedrock CaseOps Owns (Downstream)

| Responsibility | Owner |
|---|---|
| Retrieval index population | Bedrock CaseOps |
| Vector embedding and similarity search | Bedrock CaseOps |
| RAG pipeline and context assembly | Bedrock CaseOps |
| Agent reasoning, orchestration, escalation | Bedrock CaseOps |
| Case-support workflow execution | Bedrock CaseOps |
| Human-in-the-loop review for quarantined records | Bedrock CaseOps (or future shared tooling) |
| Cross-case analytics and KPI dashboards | Bedrock CaseOps (or out of scope for both in V1) |
| Export file consumption cadence and polling | Bedrock CaseOps |
| Retry and backfill on downstream failure | Bedrock CaseOps |

**The handoff boundary is the materialized export payload file at:**

```
/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json
```

This repo writes it. Bedrock CaseOps reads it. Neither system crosses the other's boundary.

---

## 3. V1 Handoff Unit Definition

The V1 handoff unit is a **single JSON file per export-ready Gold record**, materialized at a deterministic path in the Unity Catalog Volume.

### What Constitutes a Valid Handoff Unit

A valid V1 handoff unit satisfies all of the following:

1. Is a well-formed JSON object conforming to the `export_payload` structure defined in § 4
2. Corresponds to a Gold record with `export_ready = true`
3. Has a `routing_label` from the defined taxonomy that maps to a named downstream consumer (§ 5)
4. Has `document_type_label != 'unknown'`
5. Has a `document_id` with a complete, verifiable Bronze → Silver → Gold lineage chain
6. Carries a `schema_version` field matching the contract version under which it was produced

### What Is NOT a Handoff Unit

The following are never materialized as export payload files and must not be treated as handoff units:

- Gold records with `export_ready = false`
- Gold records with `routing_label = 'quarantine'`
- Silver or Bronze layer records (intermediate data; not part of the downstream contract)
- Raw document files (source-only; immutable in Volume)
- Evaluation artifacts, MLflow run logs, or intermediate pipeline outputs

---

## 4. Export Payload — Field Definitions

The `export_payload` is the structured JSON object embedded in every `export_ready` Gold record and also materialized as a standalone JSON file at the export path.

### 4.1 Required Fields

These fields must be present in every valid handoff unit. Absence of any required field is grounds for the record to be marked `export_ready = false`.

| Field | Type | Description |
|---|---|---|
| `document_id` | string (UUID v4) | Stable cross-layer document identifier; assigned at ingest; immutable |
| `source_file` | string | Original filename as uploaded to the Volume |
| `document_type` | string | Classification label from the defined taxonomy |
| `routing_label` | string | Target downstream consumer/workflow |
| `extracted_fields` | object | Domain-specific structured fields from the Silver layer (shape varies by `document_type`) |
| `parsed_text_excerpt` | string | First 2000 characters of the parsed text; provides raw document context for Bedrock retrieval |
| `provenance` | object | Traceability metadata (see § 4.3) |

### 4.2 Optional Fields

These fields are included when available. Their absence does not invalidate the handoff unit.

| Field | Type | Description |
|---|---|---|
| `page_count` | integer | Number of pages in the source document, if determinable |
| `char_count` | integer | Total character count of the full parsed text |
| `extraction_prompt_id` | string | Identifier of the prompt template used for extraction |

### 4.3 Provenance Object

The provenance object is **required** in every export payload. It provides traceability from the handoff unit back to the pipeline execution that produced it.

| Field | Type | Required | Description |
|---|---|---|---|
| `ingested_at` | string (UTC ISO 8601) | Yes | Timestamp when the source file was registered in the pipeline |
| `pipeline_run_id` | string | Yes | MLflow run ID of the batch that produced this record; `bootstrap_sql_v1` for bootstrap-origin records |
| `extraction_model` | string | Yes | Model identifier used by `ai_extract` |
| `classification_model` | string | Yes | Model identifier used by `ai_classify` |
| `classification_confidence` | float or null | Target: Yes; Bootstrap: Null | `ai_classify` confidence score (0.0–1.0). **Null in the A-3B bootstrap path** — see § 9 Known Limitations |
| `schema_version` | string | Yes | Contract version this payload was written against |

### 4.4 Extracted Fields by Document Type

`extracted_fields` shape is determined by `document_type`. Bedrock CaseOps consumers must select the appropriate schema based on this field.

**V1 — FDA Warning Letter (`document_type = 'fda_warning_letter'`):**

| Field | Type | Required in Payload | Description |
|---|---|---|---|
| `issuing_office` | string | Yes | FDA office that issued the warning |
| `recipient_company` | string | Yes | Company or entity receiving the warning |
| `recipient_name` | string | No | Individual named in the letter, if any |
| `issue_date` | string (ISO 8601 date) | Yes | Date the warning letter was issued |
| `violation_type` | array[string] | Yes | Categories of violation cited |
| `cited_regulations` | array[string] | No | Specific regulation identifiers cited |
| `corrective_action_requested` | boolean | Yes | Whether corrective action is explicitly demanded |
| `response_deadline_days` | integer | No | Number of days given to respond, if stated |
| `product_involved` | string | No | Product or product line named in the warning |
| `summary` | string | No | Brief extracted summary of the warning content |

**V2+ document types** (defined in schema, not yet executable end-to-end):
- CISA advisory: `advisory_id`, `severity_level`, `cve_ids`, `remediation_available`, etc.
- Incident report: `incident_date`, `incident_type`, `severity`, `root_cause`, etc.
- Field definitions for these types are in `docs/data-contracts.md` § 3.

### 4.5 Complete V1 Payload Example (Target-State)

The following represents a valid, complete V1 export payload in the target state (i.e., with `classification_confidence` populated):

```json
{
  "document_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "source_file": "fda_warning_letter_2024_001.pdf",
  "document_type": "fda_warning_letter",
  "routing_label": "regulatory_review",
  "extracted_fields": {
    "issuing_office": "Office of Pharmaceutical Quality",
    "recipient_company": "Acme Pharma Inc.",
    "recipient_name": null,
    "issue_date": "2024-03-15",
    "violation_type": ["Current Good Manufacturing Practice"],
    "cited_regulations": ["21 CFR 211.68", "21 CFR 211.100"],
    "corrective_action_requested": true,
    "response_deadline_days": 15,
    "product_involved": "Generic Ibuprofen Tablets",
    "summary": "FDA cited multiple cGMP violations at the manufacturing facility related to process controls and record-keeping requirements."
  },
  "parsed_text_excerpt": "WARNING LETTER\n\nIssued: March 15, 2024\nTo: Acme Pharma Inc.\n\nDear Mr. Smith...",
  "provenance": {
    "ingested_at": "2024-03-20T14:30:00Z",
    "pipeline_run_id": "mlflow-run-abcdef123456",
    "extraction_model": "ai_extract/v1",
    "classification_model": "ai_classify/v1",
    "classification_confidence": 0.94,
    "schema_version": "v0.1.0"
  }
}
```

**Bootstrap-origin payload note**: In the A-3B bootstrap path, `classification_confidence` will be `null` and `pipeline_run_id` will be `"bootstrap_sql_v1"`. Both are explicitly documented limitations — see § 9.

---

## 5. Routing Label → Bedrock Consumer/Workflow Mapping

The `routing_label` in every Gold record and export payload determines the intended downstream Bedrock CaseOps consumer. This mapping is the routing contract between the two systems.

| Routing Label | Bedrock Consumer / Workflow | Trigger Condition | V1 Execution Status |
|---|---|---|---|
| `regulatory_review` | Bedrock regulatory intelligence index | `document_type_label = 'fda_warning_letter'` (and future regulatory document types) | **V1 active** — FDA warning letters only |
| `security_ops` | Bedrock security operations index | `document_type_label = 'cisa_advisory'` | Planned (V2+) — label defined, no live end-to-end path |
| `incident_management` | Bedrock incident management workflow | `document_type_label = 'incident_report'` | Planned (V2+) — label defined, no live end-to-end path |
| `quality_management` | Bedrock quality assurance workflow | `document_type_label = 'quality_audit_record'` or `'standard_operating_procedure'` | Planned (V2+) — labels defined, no live end-to-end path |
| `knowledge_base` | General Bedrock knowledge base index | `document_type_label = 'technical_case_record'` | Planned (V2+) — label defined, no live end-to-end path |
| `quarantine` | Human review queue — NOT forwarded to Bedrock | Any record failing quality thresholds or classified as `unknown` | Active — governance path only |

### Routing Label Semantics

- `routing_label` is always set on every Gold record — it is never null
- The `quarantine` label signals governance rejection; such records have `export_ready = false` and are **never materialized** as export payload files
- All non-`quarantine` routing labels indicate `export_ready = true` (subject to quality threshold satisfaction)
- The routing label is immutable once written to a Gold record; reprocessing generates a new Gold record with a new `gold_record_id`
- Non-quarantine routing labels for V2+ document types are defined here to allow Bedrock CaseOps to plan for them without requiring contract renegotiation when V2+ pipelines are enabled

### V1 Routing Constraint

In V1, only the `regulatory_review` path is executable end-to-end. The other non-quarantine labels are present in the taxonomy and schema but have no live downstream Bedrock consumer and no end-to-end pipeline validation for those document types. Implementing them is V2+ scope.

---

## 6. `export_ready` and `quarantine` Semantics

### When `export_ready = true`

A Gold record is marked `export_ready = true` only when all of the following hold:

| Condition | Layer |
|---|---|
| `document_type_label != 'unknown'` | Gold |
| `routing_label != 'quarantine'` | Gold |
| All required `export_payload` fields are structurally present | Gold |
| Corresponding Silver `validation_status` ∈ `{'valid', 'partial'}` | Silver |
| Corresponding Silver `field_coverage_pct >= 0.5` | Silver |
| *(Target-state only)* `classification_confidence >= 0.7` | Gold |

The confidence threshold applies only when `classification_confidence` is non-null. See § 9 for the bootstrap-path exception.

### When `export_ready = false`

A Gold record is marked `export_ready = false` when any of the following hold:

| Condition | Implication |
|---|---|
| `document_type_label = 'unknown'` | Classifier could not identify document type |
| Silver `validation_status = 'invalid'` | Required extraction fields are missing or malformed |
| Silver `field_coverage_pct < 0.5` | Extraction missed more than half of expected fields |
| *(Target-state only)* `classification_confidence < 0.7` | Classifier confidence below acceptable threshold |
| Any required `export_payload` field is absent | Payload is structurally incomplete |

Records with `export_ready = false`:

- Are written to the Gold table in full with all captured fields intact
- Are **not** materialized as JSON export files
- Are always assigned `routing_label = 'quarantine'`
- Remain available for reprocessing or human review
- Count toward the `quarantine_rate` metric in `eval_gold.py`

### Quarantine is a Governance Signal, Not a Failure

A quarantine rate greater than zero is expected and correct behavior. Quarantine confirms that quality gates are functioning. In the A-3B bootstrap validation, 1 of 4 documents (25%) was quarantined — this was the correct outcome for `fda_warning_letter_sample_04_daewoo.pdf`, which the classifier returned `unknown` for. The routing logic functioned as designed.

---

## 7. V1 Delivery Mechanism Semantics

### Current V1 Delivery: Structured File Export

In V1, Gold export-ready records are delivered as individual JSON files written to a Unity Catalog Volume path at Gold classification time.

**Export path pattern:**

```
/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json
```

**Examples:**

```
/Volumes/caseops/gold/exports/regulatory_review/a1b2c3d4-e5f6-7890-abcd-ef1234567890.json
/Volumes/caseops/gold/exports/security_ops/b2c3d4e5-f6a7-8901-bcde-fa2345678901.json
```

**V1 delivery properties:**

| Property | Value |
|---|---|
| One file per record | Yes — one JSON file per `export_ready = true` Gold record |
| File name | `<document_id>.json` — stable and deterministic |
| File content | Exactly the `export_payload` object as defined in § 4 |
| Write timing | At Gold classification time, atomically |
| Overwrites | Not performed — reprocessing produces a new `document_id` and a new file |
| Quarantine records | No file written — `export_ready = false` records produce no export file |

**What Bedrock CaseOps is responsible for in V1:**

- Discovering new export files (polling, batch scan, or future event notification)
- Reading and parsing the JSON payload
- Routing internally based on `routing_label`
- Handling unexpected payload shapes or missing optional fields gracefully
- Managing its own retry and backfill logic

**What Bedrock CaseOps must not assume:**

- That the pipeline will notify it of new files in real time
- That old files will be deleted or replaced
- That `classification_confidence` will always be non-null (see § 9)

### Planned V2 Delivery Mechanisms (Not in B-0 Scope)

| Mechanism | Description | Phase |
|---|---|---|
| Delta table subscription | Bedrock reads directly from `caseops.gold.ai_ready_assets` via Delta Sharing | V2+ |
| API push | Structured HTTP push to a Bedrock ingestion endpoint | V2+ |
| Event notification | CDC event or Volume trigger to initiate downstream indexing | V2+ |

These are future evolution items. None are in scope for B-0 or Phase B contract work.

---

## 8. Versioning Expectations

### Schema Version Field

Every record and export payload carries `schema_version`. Current contract version: **`v0.1.0`**.

| Version Component | Increment Trigger |
|---|---|
| Patch (0.1.**x**) | Documentation-only; no schema field changes |
| Minor (0.**x**.0) | Additive: new optional fields, new routing labels, new document type labels |
| Major (**x**.0.0) | Breaking: field removal, type changes, renamed required fields, semantic changes |

### Compatibility Commitments

**This repo commits to:**

- Notifying Bedrock CaseOps before any breaking schema change (major version increment)
- Marking deprecated fields with a `deprecated_in` annotation before removal
- Incrementing `schema_version` on every structural change, including additive ones

**Bedrock CaseOps is expected to:**

- Handle unknown optional fields gracefully (forward-compatible parsing)
- Use `schema_version` as the signal for its own schema migration handling
- Not assume field presence beyond what is marked Required in this contract

### V1 Contract Stability Commitment

The V1 contract (this document, version `v0.1.0`) is considered stable for the duration of Phase B. Contract revisions before Phase B completion require a new B-0 revision. Version increments are tracked in the document header and in `docs/data-contracts.md`.

---

## 9. Known Current Limitations

These limitations are explicitly acknowledged. They must not be treated as resolved until the relevant phase or fix is complete.

| Limitation | Detail | Current Status |
|---|---|---|
| `classification_confidence` is null in bootstrap-origin records | The A-3B bootstrap SQL path does not expose scalar confidence from `ai_classify`. Provenance `classification_confidence` is `null` for these records. | Known gap in bootstrap SQL path; target-state will populate this field |
| `pipeline_run_id = 'bootstrap_sql_v1'` in bootstrap-origin records | A-3B records use a static placeholder, not a real MLflow run ID. `document_id`-based lineage is intact regardless. | Known; target-state MLflow pipeline will produce real run IDs |
| Only `regulatory_review` routing path is V1-executable | All other non-quarantine routing labels are defined but map to no live downstream consumer and have no end-to-end validated pipeline | By design; V2+ scope |
| No live Bedrock CaseOps consumer exists | Bedrock CaseOps does not yet have a live integration consuming from the export path. This contract defines what it will consume. | Expected at B-0 contract establishment; live integration is Phase B proper |
| V1 delivery is file-based only | No push notification, CDC event, or streaming mechanism. Bedrock CaseOps must discover files by polling or batch scan. | By design for V1; streaming is V3+ scope |
| Export confidence threshold not applied in bootstrap path | Bootstrap-origin records are marked `export_ready` based on label routing alone, not the full confidence-inclusive threshold | Explicitly documented in `docs/data-contracts.md` § 7 |
| Multi-document class routing is not validated | The A-3B batch contained only FDA warning letters. CISA advisories, incident reports, and other V2+ document types are untested. | By design for V1 |
| No human review workflow for quarantined records | Quarantined records sit in the Gold table and are not automatically routed to any review tool | V2+ scope |

---

## 10. Acceptance Criteria for B-0

B-0 is complete when all of the following criteria are verifiably satisfied:

| Criterion | Verification Method |
|---|---|
| This contract document is committed to the repo | `docs/bedrock-handoff-contract.md` present |
| Required vs optional payload fields are explicitly differentiated | § 4.1 and § 4.2 of this document |
| Every routing label maps to a named downstream consumer | § 5 of this document |
| `export_ready = true` and `export_ready = false` conditions are unambiguous | § 6 of this document |
| Quarantine semantics are explicit | § 6 of this document |
| V1 delivery mechanism is precisely defined | § 7 of this document |
| Versioning expectations are stated | § 8 of this document |
| Known limitations are honest and explicit | § 9 of this document |
| `ARCHITECTURE.md` Bedrock Handoff Design section references this contract | Updated `ARCHITECTURE.md` |
| `docs/data-contracts.md` `export_payload` is consistent with this contract | Updated `data-contracts.md` |
| `PROJECT_SPEC.md` B-0 entry reflects contract-hardening scope (not live integration) | Updated `PROJECT_SPEC.md` |
| `docs/roadmap.md` B-0 section has concrete deliverables and completion criteria | Updated `roadmap.md` |
| No committed document claims Bedrock CaseOps integration is live | Verified across all updated files |
| Databricks / Bedrock ownership split is unambiguous | § 2 of this document; consistent across all files |

---

## 11. Out of Scope for B-0

The following are explicitly excluded from B-0 and must not be treated as delivered by this phase:

| Out of Scope | Rationale |
|---|---|
| AWS credentials, IAM roles, or S3 configuration | Production credentials never in this repo; not a Databricks-layer concern |
| Bedrock SDK code or API client implementation | Bedrock CaseOps implementation; not this repo |
| Live ingestion endpoint on the Bedrock side | Phase B proper |
| Vector embedding or retrieval index configuration | Bedrock CaseOps owns retrieval |
| Agent reasoning logic or escalation rules | Bedrock CaseOps owns orchestration |
| Multi-domain pipeline execution (V2+ document types) | V2+ scope |
| Streaming or event-driven delivery | V3+ scope |
| Human-in-the-loop review tooling | V2+ scope |
| Delta Sharing configuration | V2+ delivery mechanism |
| MLflow experiment population with live metrics | Requires live Databricks pipeline execution |
| Production Databricks deployment or Asset Bundles | Not in scope for this portfolio-safe project |
