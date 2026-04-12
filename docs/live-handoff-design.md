# Live Handoff Design — Phase C-0: Integration Delivery Mechanism Design

> **Phase**: C-0 — Integration Delivery Mechanism Design
> **Status**: C-0 design complete. C-1 producer-side implementation complete. C-2 runtime validation layer complete (producer-side validation implemented; live workspace provisioning pending).
> **Authoritative scope**: [`PROJECT_SPEC.md`](../PROJECT_SPEC.md)
> **Authoritative technical design**: [`ARCHITECTURE.md`](../ARCHITECTURE.md)
> **Upstream contract**: [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md)

---

## 1. Purpose

This document is the primary C-0 design artifact for the Databricks CaseOps Lakehouse V2 live handoff integration. It:

- Formally records the C-0 delivery mechanism selection decision
- Defines the runtime boundary between Databricks CaseOps (this repo) and Bedrock CaseOps
- Resolves the "replace or augment" wording tension present in V1 documents
- Establishes the implementation target for Phase C-1
- Establishes the validation target for Phase C-2
- Documents consumer-side prerequisites and V2 contract versioning implications
- Records what C-0 explicitly does not address

This document does not describe implemented code. It records a design decision that makes C-1 implementation-ready.

---

## 2. Why C-0 Exists Now

V1 (Phases A-0 through B-6 plus the MLflow live-workspace checkpoint) delivered:

- A complete, single-domain (FDA warning letters) document intelligence pipeline validated in a real Databricks personal workspace
- A contract-enforced Gold → Bedrock handoff layer: B-0 contract, B-1 enforced validator, B-2 materialization gate, B-3 service boundary, B-4 outcome observability, B-5 batch manifest, B-6 bundle integrity validation
- A proven V1 delivery unit: one JSON file per `export_ready = true` Gold record, materialized at `/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json`

**What V1 did not deliver**: a live delivery mechanism. The V1 export path writes payloads to a Unity Catalog Volume and stops there. A downstream consumer must manually locate, poll, or copy those files. This is intentional for V1 — the goal was to establish a validated, contract-enforced payload, not a live delivery channel.

V2-C must move across the handoff boundary. Before writing any live delivery code (C-1), C-0 is required to:

1. Select the delivery mechanism from the defined candidate set
2. Confirm that the selection does not corrupt the Databricks / Bedrock ownership split
3. Resolve the "replace or augment" ambiguity in V1 docs
4. Establish what Bedrock CaseOps must implement to receive
5. Confirm whether the V2 contract requires a version increment
6. Define what a successful C-1 implementation looks like so that C-2 validation is testable

---

## 3. Current Validated V1 Handoff Baseline

The following is the validated state entering V2-C. This is not a plan — it is factual confirmed status.

| Component | Status | Detail |
|---|---|---|
| Export payload structure | Validated | `export_payload` with required/optional fields per `docs/bedrock-handoff-contract.md` §4 |
| Contract enforcement | Live in pipeline | `bedrock_contract.py` gates every export write; invalid payloads blocked |
| Export materialization path | Deterministic | `/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json` |
| Gold Delta table | Confirmed in personal workspace | `caseops.gold.ai_ready_assets` with full lineage |
| Batch manifest (B-5) | Implemented | `HandoffBatchManifest` references all per-record artifact paths per pipeline run |
| Bundle integrity validation (B-6) | Implemented | 24 checks; proves the batch bundle is internally consistent and trustworthy |
| Contract version | v0.1.0 | Declared in `schema_version` field of every export payload |
| Live Bedrock integration | Does not exist | No AWS, no Bedrock SDK, no live endpoint — V1 never crossed the boundary |

**The V1 delivery boundary**: This repo writes a contract-valid JSON payload file to a Unity Catalog Volume path. Nothing more. Bedrock CaseOps has not been implemented. No live delivery has occurred.

---

## 4. V2 Problem Statement

V1 is functionally correct but incomplete as a delivery system. Specifically:

**The handoff is file-materialization-only**: Payloads exist on disk at a well-known Volume path, but there is no mechanism to:
- Notify Bedrock CaseOps that new payloads are ready
- Deliver payloads across a system boundary without a manual copy step
- Confirm delivery receipt on the consumer side
- Provide the consumer governed, auditable access to the Gold layer without filesystem coupling

**The consumer coupling is implicit**: Bedrock CaseOps must know where to look (`/Volumes/caseops/gold/exports/...`), must poll for new files, and has no delivery acknowledgment path. This is adequate for V1 (no consumer exists yet) but is architecturally fragile as a long-term delivery model.

**Multi-domain scaling amplifies the problem**: Phase D will add CISA advisories and incident reports. A file-poll model becomes harder to govern at scale. A proper delivery mechanism should accommodate multi-domain payloads with routing transparency.

V2-C must select and implement a delivery mechanism that:
- Crosses the handoff boundary in a governed, traceable way
- Does not require Bedrock CaseOps to poll a filesystem
- Preserves the B-phase contract enforcement layer
- Is realistic to implement in a personal/non-production portfolio context
- Maintains the Databricks / Bedrock ownership split cleanly

---

## 5. Delivery Mechanism Comparison

Three candidate mechanisms are evaluated below. Each is assessed against the same criteria. A recommendation follows in §6.

### Evaluation Criteria

| Criterion | Weight | Notes |
|---|---|---|
| Fit with current repo architecture | High | Must work with Unity Catalog, Gold Delta table, existing export path |
| Governance and traceability | High | Unity Catalog lineage must be preserved |
| Implementation realism (personal/non-production) | High | No enterprise infra, no production credentials |
| Consumer contract clarity | High | Bedrock CaseOps must have an unambiguous consumption interface |
| Operational observability | Medium | Delivery health must be detectable |
| Coupling to Bedrock-side implementation | Medium | Prefer lower coupling |
| Extensibility for multi-domain (Phase D) | Medium | Must accommodate CISA, incident report payloads |
| Preservation of V1 file/bundle artifacts | Medium | B-phase work should not be discarded |
| Portfolio credibility | Medium | Must demonstrate real Databricks platform capability |
| Failure handling clarity | Medium | What happens when delivery fails |

---

### Option A — Delta Sharing

**What it is**: Databricks Delta Sharing is a native, open-protocol mechanism for sharing Delta tables with external recipients. A share is created in Unity Catalog; a recipient (e.g., Bedrock CaseOps) is granted access; the recipient queries the shared table directly using the Delta Sharing protocol. No data is copied; access is governed and auditable.

**In this context**: Share the `caseops.gold.ai_ready_assets` Delta table (and optionally the B-5 `handoff_bundle` artifacts) with a Bedrock CaseOps recipient. Bedrock queries the share directly to discover new `export_ready = true` records. The V1 file materialization path is **augmented**, not replaced — files remain as the contractual payload unit; the Delta Share provides governed table-level access to batch metadata and routing signals.

| Criterion | Assessment |
|---|---|
| Fit with current repo architecture | **Excellent** — Unity Catalog already manages the Gold table; Delta Sharing is a native UC extension |
| Governance and traceability | **Excellent** — Delta Sharing is governed at the Unity Catalog level; access is logged; share recipients are explicit |
| Implementation realism | **Good** — Personal workspace supports Delta Sharing (with recipient token-based access); no cloud-to-cloud infra required |
| Consumer contract clarity | **Good** — The shared table schema is the interface; schema_version is visible at the table level |
| Operational observability | **Good** — Share access can be monitored; table-level audit logs available |
| Coupling to Bedrock-side implementation | **Low** — Bedrock CaseOps accesses the share on its own schedule; no push coupling |
| Extensibility for multi-domain | **Excellent** — Additional routing labels and document types are visible in the same shared table without protocol changes |
| Preservation of V1 file/bundle artifacts | **Excellent** — Delta Sharing augments the file path; B-phase artifacts are unchanged |
| Portfolio credibility | **Excellent** — Delta Sharing is a distinctive, Databricks-native capability not commonly demonstrated in portfolio projects |
| Failure handling clarity | **Good** — Bedrock-side access failures are isolated; upstream write failures are already handled by B-6 validation |

**Risks / Considerations**:
- Delta Sharing recipient setup requires a share token and recipient configuration — this is non-trivial in a personal workspace but achievable
- Consumer must support the Delta Sharing protocol (the `delta-sharing` open Python client library covers this)
- Shared table access does not push payloads — Bedrock must query the share; this is pull, not push, which is architecturally desirable but slightly less "live" than a push model
- In a non-production portfolio context, "the recipient" can be a self-share or a simulated consumer

**Verdict**: Strongest overall option for a Databricks-native portfolio. Architecturally clean, governance-first, and realistic.

---

### Option B — Structured API Push

**What it is**: After batch materialization, this repo pushes export payloads to an HTTP endpoint owned and operated by Bedrock CaseOps. Each `export_ready = true` payload is sent as a POST request. The Bedrock consumer registers receipt. Success/failure is tracked per payload.

**In this context**: After `classify_gold.py` writes a payload, a new delivery module sends it to a Bedrock-controlled endpoint. The B-5 batch manifest could serve as the push manifest. Contract enforcement (B-1) already validates payloads before write; the push would happen post-validation.

| Criterion | Assessment |
|---|---|
| Fit with current repo architecture | **Poor** — Introduces HTTP client logic and live endpoint dependency into the upstream pipeline; alien to Databricks-native pattern |
| Governance and traceability | **Fair** — Requires custom logging of push outcomes; not governed by Unity Catalog |
| Implementation realism | **Poor** — Requires a live HTTP endpoint on the Bedrock side; in a personal portfolio context, this means standing up a real API or running a mock — adds significant infra burden |
| Consumer contract clarity | **Good** — Request/response semantics are explicit |
| Operational observability | **Fair** — Push success/failure requires custom instrumentation |
| Coupling to Bedrock-side implementation | **High** — This repo cannot deliver without the Bedrock endpoint being live and reachable; tight coupling |
| Extensibility for multi-domain | **Fair** — Each new domain adds payload types; the API contract must evolve with domain expansion |
| Preservation of V1 file/bundle artifacts | **Fair** — Could be built on top of the V1 file path, but the API push introduces a separate delivery layer that may diverge |
| Portfolio credibility | **Poor** — API push is the least distinctive choice; it does not demonstrate Databricks-native capability; it reduces this repo to a generic REST client |
| Failure handling clarity | **Poor** — Push failures require retry logic, dead-letter handling, and endpoint availability management — none of which belong in this repo |

**Verdict**: **Rejected.** Structured API push introduces tight coupling, requires live infra on both sides, is not Databricks-native, and does not demonstrate the platform's governed delivery capabilities. It also imports Bedrock operational concerns (endpoint availability, retry logic) into this repo's scope — a clear ownership split violation.

---

### Option C — Event-Driven Notification with Export Manifest Reference

**What it is**: After each batch pipeline run, an event is emitted (e.g., written to a Databricks notification queue, a Delta table `delivery_events` row, or a watched Volume path) pointing to the B-5 batch manifest. Bedrock CaseOps subscribes to or polls for these events, then fetches the referenced payloads from the well-known Volume path using the manifest as the index.

**In this context**: The B-5 `HandoffBatchManifest` already links all per-record artifact paths. The delivery event is essentially a pointer to this manifest. Bedrock CaseOps consumes the event, opens the manifest, and fetches payloads at its own pace. The V1 file path is augmented: files stay at their Volume paths; an event layer is added to notify consumers.

| Criterion | Assessment |
|---|---|
| Fit with current repo architecture | **Good** — The B-5 manifest is already the natural delivery handshake document; the event layer is a thin addition |
| Governance and traceability | **Fair** — Depends on the event mechanism; a Delta table delivery events log would be governed; a file-watcher approach is less formal |
| Implementation realism | **Good** — Can be implemented with a Delta table `delivery_events` table in Unity Catalog; no external queue infra required |
| Consumer contract clarity | **Good** — Manifest structure is already defined; the event payload is the manifest path |
| Operational observability | **Good** — Delivery events table provides an audit trail of what was notified and when |
| Coupling to Bedrock-side implementation | **Medium** — Consumer must implement event subscription or polling; looser than API push but tighter than Delta Sharing |
| Extensibility for multi-domain | **Good** — Event payload includes `routing_label`; Bedrock can filter by domain |
| Preservation of V1 file/bundle artifacts | **Excellent** — This option is entirely additive; B-phase artifacts are completely unchanged |
| Portfolio credibility | **Fair** — Event-driven delivery is a credible pattern, but is less distinctively Databricks-native than Delta Sharing |
| Failure handling clarity | **Good** — Event emission failure is separable from payload write failure; retryable |

**Risks / Considerations**:
- A true event system (Kafka, EventBridge) is out of scope for a personal portfolio project
- A Delta table `delivery_events` implementation is realistic and governed, but it is still a pull model dressed as push
- Less architecturally distinctive than Delta Sharing for a Databricks portfolio

**Verdict**: **Valid as a secondary or complementary mechanism, not the primary delivery channel.** The event-driven manifest pointer pattern is additive and realistic, but it does not leverage Databricks platform capabilities as clearly as Delta Sharing. It is a reasonable complement: Delta Sharing governs the table-level access; a delivery events table records the per-batch notification history.

---

## 6. Recommended Delivery Mechanism

**Primary recommendation: Delta Sharing, augmenting the V1 file export path.**

**Secondary complement: Delivery events table (Delta table in Unity Catalog) recording per-batch delivery notifications.**

**Rejected: Structured API push.**

### Recommendation Rationale

**Delta Sharing is recommended as the primary mechanism because:**

1. It is genuinely Databricks-native — it uses Unity Catalog governance, not external infra
2. It adds governed, auditable consumer access to the Gold layer without copying data
3. It preserves and complements the V1 file export path — the files remain the contractual payload unit; Delta Sharing adds the table-level access layer
4. It is implementable in a personal workspace without production credentials
5. It directly demonstrates Databricks platform capability, which is the core portfolio claim of this repo
6. The coupling to Bedrock CaseOps is minimal — Bedrock queries the share on its own schedule; this repo does not need Bedrock to be live to materialize payloads
7. Multi-domain expansion (Phase D) is transparent — new routing labels and document types appear in the same shared table without protocol changes

**The V1 file export path is retained (augment, not replace) because:**

1. The B-1 through B-6 contract enforcement, materialization, reporting, and bundle validation work is built on the file export path — discarding it wastes validated, tested implementation
2. The file export artifact is the contractual unit as defined in `docs/bedrock-handoff-contract.md` §3 — it is the precise payload shape that Bedrock CaseOps must consume
3. Delta Sharing provides governed access to the Gold Delta table metadata; the file export provides the exact payload — both have roles
4. For V2 multi-domain expansion, the file-per-document pattern scales cleanly with routing label subdirectories already in place

**The structured API push is rejected because:**
- It introduces tight temporal coupling (Bedrock must be live when this repo pushes)
- It imports Bedrock operational concerns (endpoint availability, retry, dead-letter) into this repo's scope
- It is not Databricks-native and does not demonstrate platform capability
- It would require standing up live HTTP infrastructure in a personal portfolio context

### C-1 Implementation Status (Complete — April 2026)

C-1 implements the **upstream producer-side delivery augmentation** on top of the existing V1 file export path. The following has been implemented:

| Deliverable | Path | Status |
|---|---|---|
| Delivery event schema (Pydantic) | `src/schemas/delivery_event.py` | ✅ Implemented |
| Delivery event materialization | `src/pipelines/delivery_events.py` | ✅ Implemented |
| Delta Share prep layer + SQL templates | `src/pipelines/delta_share_handoff.py` | ✅ Implemented |
| v0.2.0 provenance fields in `ExportProvenance` | `src/schemas/gold_schema.py` | ✅ Updated |
| `--delivery-dir` integration in pipeline | `src/pipelines/classify_gold.py` | ✅ Updated |
| Expected delivery event fixture | `examples/expected_delivery_event.json` | ✅ Added |
| Delivery event tests (88 tests) | `tests/test_delivery_events.py` | ✅ Added |
| Delta Share handoff tests (67 tests) | `tests/test_delta_share_handoff.py` | ✅ Added |

**C-1 implementation stance**: All delivery events carry `status = 'prepared'`. This means the producer-side layer is complete. No live Unity Catalog provisioning has been executed — that remains a manual step or C-2 automation. The C-1 delivery layer generates SQL DDL templates via `DeltaShareConfig` and `generate_share_setup_sql()` that can be executed in a Databricks SQL notebook.

**What C-1 does NOT deliver** (deferred to C-2):
- Live Unity Catalog Delta Share creation
- Recipient configuration and access token issuance
- End-to-end delivery receipt confirmation
- Bedrock CaseOps consumer simulation

**C-1 decision on item 3 (from C-0 deferred list)**: Open sharing vs. recipient sharing — deferred to C-2. The `DeltaShareConfig` defaults to recipient-based sharing (`recipient_name = 'bedrock_caseops'`); the generated SQL includes both `CREATE RECIPIENT` and `GRANT` statements. Personal workspace open sharing is also supported by omitting the recipient block.

C-1 is equivalent to items 5–8 in the original plan (delivery events write, manifest reference, delivery module). Items 1–4 (actual Unity Catalog share provisioning) are C-2 / manual steps.

Full C-1 scope is defined in §11 below.

---

## 7. Databricks / Bedrock Runtime Boundary Definition

This section provides the definitive V2 runtime boundary statement. It supersedes the partial descriptions in other documents where they describe V1 only.

### The Boundary Artifact

The handoff boundary is defined by two artifacts that are produced by this repo and consumed by Bedrock CaseOps:

| Artifact | Path / Location | Role |
|---|---|---|
| **Export payload file** | `/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json` | The contractual handoff unit; exact payload for one document; produced by `export_handoff.py` |
| **Delta Share** (V2) | `caseops_handoff` share → `gold_ai_ready_assets` table | Governed table-level access to Gold metadata; batch discovery; routing-aware filtering |
| **Delivery event** (V2) | `caseops.gold.delivery_events` Delta table | Per-batch delivery notification log; references B-5 manifest path; Bedrock subscribes or polls |
| **B-5 Batch manifest** | `output/reports/handoff_bundle_<run_id>.json` (local) or Volume path (Databricks) | Review and navigation index for a pipeline batch run; referenced in delivery event |

### What This Repo Owns (V2)

| Concern | Owner | Notes |
|---|---|---|
| Document ingestion, parsing, extraction, classification | This repo | Bronze → Silver → Gold pipeline |
| Schema validation and contract enforcement | This repo | B-1 validator; B-2 materialization gate |
| Export payload materialization | This repo | File at deterministic Volume path |
| Delta Share configuration and provisioning | This repo | Share + table + recipient configuration in Unity Catalog |
| Delivery event write | This repo | Written after successful batch materialization |
| Batch manifest packaging (B-5) | This repo | B-5 bundle, referenced in delivery event |
| Bundle integrity validation (B-6) | This repo | Pre-delivery consistency check |

### What Bedrock CaseOps Owns (V2)

| Concern | Owner | Notes |
|---|---|---|
| Delta Share consumption | Bedrock CaseOps | Queries `gold_ai_ready_assets` via Delta Sharing protocol |
| Delivery event subscription / polling | Bedrock CaseOps | Reads `delivery_events` table to discover new batches |
| Export payload file fetch | Bedrock CaseOps | Reads file at `/Volumes/caseops/gold/exports/...` path from manifest |
| Retrieval index population | Bedrock CaseOps | Embeds and indexes consumed payloads |
| Vector search and RAG | Bedrock CaseOps | Not this repo at any phase |
| Agent reasoning, escalation, case-support workflows | Bedrock CaseOps | Not this repo at any phase |
| Delivery receipt acknowledgment | Bedrock CaseOps | Consumer-side confirmation; not written back to this repo |
| Retry on consumer-side failure | Bedrock CaseOps | This repo does not retry on consumer behalf |

### Hard Boundary Rules (V2 Non-Negotiable)

1. This repo does not implement retrieval indexes, vector stores, or embedding models
2. This repo does not own agent workflows, RAG pipelines, or escalation logic
3. This repo does not write Bedrock SDK code or AWS-side infrastructure configuration
4. This repo does not poll for delivery acknowledgment from Bedrock
5. Delta Share provisioning is the furthest this repo crosses toward Bedrock — it makes data available; Bedrock accesses it
6. The `delivery_events` table is a write-only audit log from this repo's perspective — Bedrock reads it; this repo does not process Bedrock's read activity
7. Multi-domain expansion (Phase D) adds document types and routing labels to this repo's pipeline; it does not add retrieval or agent logic

---

## 8. Consumer-Side Assumptions

For C-1 to be testable, Bedrock CaseOps must implement or simulate the following. These are documented here for consumer planning only — this repo does not implement them.

| Consumer Prerequisite | Description | C-2 Validation Approach |
|---|---|---|
| Delta Sharing recipient configured | A valid Delta Sharing recipient token for `caseops_handoff` share | Confirm recipient can query `gold_ai_ready_assets` in C-2 |
| Delta Sharing client library installed | `delta-sharing` Python client, or Databricks SQL connector | Consumer-side prerequisite; not in this repo |
| Delivery events table accessible | Bedrock CaseOps can read `caseops.gold.delivery_events` | Confirm delivery event row is readable in C-2 |
| Export file paths resolvable | Bedrock CaseOps can resolve `/Volumes/caseops/gold/exports/...` paths from manifest | Confirm file fetch succeeds in C-2 |
| Consumer contract version awareness | Consumer knows it is consuming `schema_version: v0.2.0` payloads | Declared in contract; validated in C-2 |

**In a personal portfolio context**: "Bedrock CaseOps" can be simulated as a simple Python script or Databricks notebook that:
1. Reads the `delivery_events` table
2. Opens the referenced B-5 manifest
3. Fetches the referenced export payload files
4. Confirms payload structure matches `schema_version: v0.2.0`

This simulation is sufficient to validate C-1 delivery and define C-2 validation targets without requiring a full Bedrock system to exist.

---

## 9. Contract / Versioning Implications

### Recommended Version Bump: v0.1.0 → v0.2.0

C-0 recommends a **minor version increment** from `v0.1.0` to `v0.2.0`. The change is additive — no required field changes, no type changes, no removals.

| Change | Version Impact | Reason |
|---|---|---|
| Addition of Delta Sharing delivery channel | Minor bump | New delivery mechanism; no payload field changes |
| Addition of `delivery_events` table | Minor bump | New table; no existing schema changes |
| No changes to `export_payload` field definitions | None | All existing payload fields are unchanged |
| No changes to routing label taxonomy | None | Existing labels unchanged; new labels are Phase D |
| No changes to required/optional field rules | None | B-0 contract field rules unchanged |

**Classification**: This is a **minor bump** because:
- The `export_payload` structure is unchanged
- Existing consumers of `schema_version: v0.1.0` payloads do not need to change
- The delivery mechanism change is a transport-layer addition, not a payload schema change
- v0.2.0 signals "V2 delivery-capable" to consumers without breaking backward compatibility

### What Stays at v0.1.0

Existing export payload files materialized during V1 testing carry `schema_version: v0.1.0`. These are not retroactively updated. V2-C implementation will write `schema_version: v0.2.0` payloads.

### What Changes at v0.2.0

| Addition | v0.2.0 Status |
|---|---|
| `delivery_mechanism` field in `provenance` (optional) | Added as optional field; null in v0.1.0 payloads |
| `delta_share_name` field in `provenance` (optional) | Added as optional field; null in v0.1.0 payloads |
| `delivery_event_id` field in `provenance` (optional) | Added as optional field; null in v0.1.0 payloads |

These fields are **optional** — consumers of v0.1.0 payloads are not affected.

### Versioning Rule Applied

Per `docs/data-contracts.md` §Contract Versioning:
> Minor (0.x.0): Additive: new optional fields, new routing labels, new document type labels

This is precisely the v0.2.0 case: new optional fields in `provenance`, no breaking changes.

---

## 10. Chosen C-1 Implementation Direction

### What C-1 Delivers

**C-1 is complete (April 2026).** The following criteria have been met:

1. ✅ A `DeliveryEvent` Pydantic schema exists (`src/schemas/delivery_event.py`) with all fields from the C-0 design table
2. ✅ The pipeline writes a delivery event artifact (JSON + text) after each successful export batch when `--delivery-dir` is provided
3. ✅ The delivery event references the B-5 manifest path (`bundle_artifact_path`)
4. ✅ A `DeltaShareConfig` and `SharePreparationManifest` document the full share configuration, recipient, SQL DDL, handoff surface, and C-2 validation queries (`src/pipelines/delta_share_handoff.py`)
5. ✅ Export payloads written with `--delivery-dir` carry `schema_version: v0.2.0` and the three new optional provenance fields (`delivery_mechanism`, `delta_share_name`, `delivery_event_id`)
6. ✅ V1 file export path fully preserved — v0.1.0 behavior is unchanged when `--delivery-dir` is not provided
7. ✅ No AWS credentials, Bedrock SDK code, or external HTTP dependencies introduced
8. ✅ 155 new tests (88 delivery event + 67 Delta Share handoff); 613 total tests pass

**Still pending (C-2 concern):**
- 🔲 Live Unity Catalog Delta Share creation (requires Databricks workspace with CREATE SHARE privilege)
- 🔲 Recipient configuration and access token issuance
- 🔲 End-to-end delivery confirmation (share query, event row, manifest integrity, payload fetch)

### What C-1 Does NOT Deliver

- Live Bedrock CaseOps consumer implementation
- Retrieval index population
- Agent or RAG workflows
- Multi-domain pipelines (Phase D)
- Environment separation (Phase E-1)
- Production deployment

### New Modules (C-1 Design)

| Module / Artifact | Path (planned) | Role |
|---|---|---|
| Delta Share bootstrap SQL | `notebooks/c1/01_delta_share_setup.sql` | Creates share, adds table, creates recipient |
| Delivery events schema | `src/schemas/delivery_events_schema.py` | Pydantic schema for `delivery_events` row |
| Delivery handoff module | `src/pipelines/delivery_handoff.py` | Writes delivery event row after successful batch; augments `export_handoff.py` pattern |
| C-1 validation notebook | `notebooks/c1/02_c1_delivery_validation.sql` | Queries share and delivery events to confirm delivery path |
| Updated bedrock-handoff-contract.md | `docs/bedrock-handoff-contract.md` | v0.2.0 additions: Delta Sharing delivery section, delivery_events table spec, consumer query examples |

### `delivery_events` Table Schema (Design)

```
caseops.gold.delivery_events

| Field                | Type      | Required | Description                                         |
|----------------------|-----------|----------|-----------------------------------------------------|
| delivery_event_id    | string    | Yes      | UUID for this delivery event                        |
| batch_id             | string    | Yes      | pipeline_run_id from the batch (= B-5 manifest batch_id) |
| delivered_at         | timestamp | Yes      | UTC; time delivery event was written                |
| delivery_mechanism   | string    | Yes      | 'delta_sharing' (v0.2.0 primary)                    |
| manifest_path        | string    | Yes      | Path to B-5 HandoffBatchManifest JSON artifact      |
| total_records        | int       | Yes      | Count of export_ready records in the batch          |
| routing_labels       | array[string] | Yes  | List of routing_labels present in this batch        |
| schema_version       | string    | Yes      | 'v0.2.0' for C-1+ batches                          |
| share_name           | string    | No       | Delta Share name ('caseops_handoff')                |
| notes                | string    | No       | Optional batch-level notes                          |
```

---

## 11. Chosen C-2 Validation Direction

### What C-2 Validates

C-2 confirms that the C-1 delivery slice is end-to-end functional. C-2 is complete when:

1. A Delta Sharing recipient can query `caseops_handoff.gold_ai_ready_assets` and observe `export_ready = true` Gold records from a V2 batch
2. The `delivery_events` table shows a row for the V2 batch with correct counts and manifest path
3. The B-5 manifest referenced in the delivery event is readable and passes B-6 integrity validation
4. Export payload files referenced in the manifest are fetchable and conform to `schema_version: v0.2.0`
5. A C-2 validation notebook or script demonstrates all four of the above in sequence
6. MLflow or a structured evaluation artifact captures the delivery validation result

### C-2 Validation Targets

| Validation Target | Check | Evidence |
|---|---|---|
| Delta Share accessible | Recipient can query shared table | SQL query result showing Gold records |
| Delivery event written | Event row present in `delivery_events` | Row with matching `batch_id` |
| Manifest path valid | Manifest file exists and passes B-6 validation | `validate_handoff_bundle` result |
| Payload files fetchable | Export files at manifest-referenced paths are readable JSON | File open + JSON parse |
| Schema version correct | Payloads carry `schema_version: v0.2.0` | Field assertion |
| Routing label transparency | Shared table shows correct `routing_label` per record | Query filter on `routing_label = 'regulatory_review'` |

### C-2 Acceptance

C-2 is accepted when all six validation targets pass in a single reproducible run. The validation script or notebook must be committed to the repo. No live Bedrock system is required — a simulated consumer (Python script or Databricks notebook acting as Bedrock CaseOps) is sufficient.

---

## 12. Explicit Non-Goals of C-0 / C-Phase

The following are confirmed out of scope for the entire Phase C:

| Non-Goal | Reason |
|---|---|
| Live Bedrock CaseOps consumer implementation | Bedrock CaseOps owns its own implementation |
| Retrieval index population, vector search, RAG | Bedrock CaseOps |
| Agent reasoning, orchestration, escalation | Bedrock CaseOps |
| Multi-domain extraction (CISA, incident reports) | Phase D |
| Human review queue | Phase E-0 |
| Environment separation (dev/staging/prod) | Phase E-1 |
| Governance monitoring views | Phase E-2 |
| AWS infrastructure, IAM roles, S3 | Out of scope at all phases |
| Production credentials or enterprise deployment | Out of scope at all phases |
| Replacing the V1 file export path | C-1 augments; it does not replace |
| Streaming or near-real-time delivery | V3+ |

---

## 13. Open Questions / Deferred Items

The following items are acknowledged but explicitly deferred beyond C-0:

| Item | Deferral Point | Notes |
|---|---|---|
| Delta Sharing access token lifecycle | C-1 implementation | Recipient tokens are time-bounded; rotation is an operational concern. Personal workspace tokens are short-lived. C-1 should use environment variables, not committed credentials. |
| Delta Sharing open sharing vs. recipient sharing | C-1 implementation | Personal workspace may use open sharing (no recipient configuration required) for portfolio demonstration. Decision deferred to C-1 based on workspace capabilities. |
| `delivery_events` table partitioning | C-1 implementation | For a personal workspace with low volume, no partitioning needed. Phase E-1 can revisit. |
| Whether C-2 requires a live Databricks cluster | C-2 | SQL-based validation requires a running serverless warehouse; this is consistent with A-3B pattern. Documented as a C-2 prerequisite. |
| Delta Share for B-5 manifest artifacts | Phase D or E-2 | Including manifest artifacts in the share (not just the Gold table) is a governance enhancement. Deferred until multi-domain batch volumes justify it. |
| Consumer-side delivery confirmation write-back | Phase D or E-0 | A `delivery_acknowledgments` table where Bedrock writes receipt confirmations is a natural extension. Deferred — it crosses the ownership boundary (Bedrock writes to this repo's schema). Requires explicit design. |

---

## 14. Decision Summary

| Decision | Outcome |
|---|---|
| Primary delivery mechanism | **Delta Sharing** |
| Secondary delivery artifact | **Delivery events table** (`caseops.gold.delivery_events`) |
| Rejected mechanism | **Structured API push** |
| File export path disposition | **Retained and augmented** (not replaced) |
| Contract version change | **v0.1.0 → v0.2.0** (minor, additive) |
| Databricks / Bedrock boundary | **Unchanged in principle; Delta Share configuration added to this repo's scope** |
| C-1 implementation target | Delta Share setup + delivery events write, validated in personal workspace |
| C-2 validation target | End-to-end delivery confirmation: share query + event row + manifest integrity + payload fetch |
| New optional provenance fields | `delivery_mechanism`, `delta_share_name`, `delivery_event_id` |
