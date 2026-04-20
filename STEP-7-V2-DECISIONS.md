# Step 7 v2 — Decisions, Pre-Deploy Checklist & Open Questions

**Branch:** `claude/step-7-v2-execution-b6INF`
**Worker:** `step-7-peach-system-creative-analysis` (version `v2`)
**Reference:** `STEP-7-V2-DESIGN-BRIEF.md`, `CLAUDE-CODE-BRIEF-STEP-7-V2.md`

---

## 1. What was built

### Phase A — Prompt drafts (`step-7-v2-prompts-DRAFT.js`, 876 lines)
A standalone, reviewable ES module exporting the four updated prompt builders
plus the helper formatters they depend on. Built in chunks A1-A5c:
- A1: constants + `formatCreativeHistory` + `formatFormatAggregations`
- A2: `formatCreative` helper + `buildAnalysisPrompt`
- A3: `buildCreativeReasoningPrompt` (90-day history awareness + sufficiency gates)
- A4: `buildCreativeStructuringPrompt` (v1 plain-text block preserved, sentinel + JSON tail appended)
- A5a-c: `buildBriefPrompt` (brand tone system bit-exact from v1, analyst-voice + pattern-grounded rules)

### Phase B — Worker (`step-7-peach-system-creative-analysis-v2.js`, 1706 lines)
- B1 (`7aadeee`): copy of v1 worker, header rewritten, `/health` bumped to v2,
  constants added (`HISTORY_WINDOW_DAYS`, `HISTORY_MAX_ADS_DEFAULT`,
  `PATTERNS_JSON_DELIMITER`), two new fetches: `getCreativeHistory` (Airtable,
  90-day, spend-gated), `getBaselineContext` (D1, null-safe).
- B2 (`ed07793`): `processAccount` rewired — four parallel fetches, format
  aggregation computed from history, new inputs passed into v2 prompt builders,
  structuring response split on sentinel via new `parseStructuringResponse`,
  `writeResults` extended with two new fields.

---

## 2. Key decisions

### 2.1 Sentinel-delimited dual output (structuring call)
The v1 frontend parses `creative_analysis` as plain text (Scale These /
Consider Pausing / Watch Closely / Key Insight / Recommended Action). That
contract is preserved bit-exact.

The two new output fields (`creative_pattern_observations`,
`forward_preparation`) are emitted by the SAME structuring call, appended
after a sentinel line:

```
[v1 plain-text analysis block — unchanged]

===PATTERNS_JSON===
{"creative_pattern_observations":[...],"forward_preparation":"..." | null}
```

`parseStructuringResponse` splits on the sentinel. If the sentinel is absent
or the JSON tail is malformed, it falls back to: full response → `analysisText`,
new fields → `[]` / `null`. Frontend contract is never broken.

**Why this over a fourth Claude call:** half the tokens, no extra latency,
no extra rate-limit exposure. The reasoning is already structured by this
point; the model just tails it with the pattern observations it already
derived.

### 2.2 Inlined v2 prompt functions (not imported)
The v1 prompt functions were replaced in-file with the v2 versions from
`step-7-v2-prompts-DRAFT.js`. The draft file stays as the single reviewable
source of truth for the prompt changes; the worker carries its own copies
so deployment remains single-file-safe.

Drift risk: if the draft is updated without updating the worker (or vice
versa), the two can diverge silently. Mitigation: the draft file's header
calls this out explicitly.

### 2.3 New Airtable fields written by NAME, not field ID
`creative_pattern_observations` and `forward_preparation` are written by
field name (requires `typecast: true`, already set). Peach has not yet
created the fields and supplied IDs, so writing by name lets the worker
ship and lets the first real run verify the fields exist.

Once field IDs are provided, replace the two name-keyed writes in
`writeResults` with IDs (see pre-deploy checklist).

### 2.4 Brand tone system preserved bit-exact
All eight tones (premium/luxury/minimal, playful/fun, bold, professional,
aspirational, cultural, urgent/urgent-direct, warm/accessible default) are
byte-for-byte identical to v1's ternary chain, converted to an if/else
chain for chunked authoring. Em dashes inside the rule STRINGS remain —
those are prompt instructions to the model about CREATIVE voice output
and do not appear in framing text, so they are exempt from the "no em
dashes in framing" rule which applies only to analyst prose.

### 2.5 Graceful degradation
Every v2-added dependency has a no-op fall-back:

| Dependency                      | Missing behaviour                                |
|--------------------------------|--------------------------------------------------|
| `BASELINE_DB` binding           | `getBaselineContext` returns `null`; prompts fall back to Airtable baseline, sufficiency defaults to `'insufficient'` across all windows |
| 90-day history empty            | `formatCreativeHistory` emits a "none above threshold" line; pattern gates trip, patterns array stays empty |
| `season_tags` missing           | Resolves to `'standard'`, `seasonStatus` = `'standard'` |
| Sentinel missing in response    | Full response becomes `analysisText`; new fields default to empty; warning logged |
| JSON tail malformed             | `analysisText` preserved; new fields default to empty; warning logged |
| New Airtable fields missing     | `typecast: true` will attempt creation; if the Airtable table forbids it, those two fields are silently skipped — the rest of the record still writes |

### 2.6 Preserved from v1
- Three-call architecture (reasoning → structuring → briefs) — no new calls added.
- Objective-strict handling (leadgen vs sales vs traffic).
- Currency discipline: numbers only in framing, no symbols.
- Season status logic (algorithmic Islamic calendar).
- `MIN_SPEND_FOR_ANALYSIS = 50` threshold.
- Fatigue flag handling, hook style variety, minimum spend thresholding.
- `creative_analysis` plain-text Airtable field contract.

---

## 3. Pre-deploy checklist

### 3.1 Airtable — required before first run
- [ ] Create field `creative_pattern_observations` in
      `ai_creative_analysis_results` (tblfZwmJqj4lS01qo).
      Type: **Long text**. Content stored as JSON string
      (`[{"title":"...","detail":"...","pattern_type":"..."}]`).
- [ ] Create field `forward_preparation` in the same table.
      Type: **Long text**. Content: 1-2 analyst-voice sentences or empty.
- [ ] Provide field IDs for both — update `writeResults` in the worker to
      write by ID instead of name (grep for
      `creative_pattern_observations` / `forward_preparation` in
      `step-7-peach-system-creative-analysis-v2.js`).

### 3.2 D1 binding — required
- [ ] Confirm `BASELINE_DB` binding is configured on the Worker
      (same binding Step 6 v2 uses). Without it, sufficiency flags
      default to `insufficient` across all windows and the worker will
      degrade to Airtable baseline only — v2 pattern observations will
      be sparse.
- [ ] Confirm the D1 `baseline_context` table has rows for every active
      account for the week being analysed.

### 3.3 Deployment
- [ ] Deploy as the same Worker name (`step-7-peach-system-creative-analysis`)
      — replaces v1.
- [ ] Hit `/health` — expect `{"status":"ok","step":7,"version":"v2", ...}`.
- [ ] Run `/run-single?account_id=<id>` against one low-risk account; verify:
      - `creative_analysis` renders unchanged in the frontend
      - new fields populate in Airtable (or log warns cleanly if they don't)
      - 3 briefs write with brand-tone-appropriate voice
- [ ] Only after single-account smoke test passes, let the Wednesday cron run.

### 3.4 Rollback plan
If v2 misbehaves, redeploy v1 (unchanged on disk as
`step-7-peach-system-creative-analysis-v1.js`). v2 writes the same primary
fields v1 does, plus two additive fields. No schema migration required.

---

## 4. Acceptance criteria — self-check

| Criterion (from Design Brief §11)                   | Status |
|------------------------------------------------------|--------|
| 90-day creative history reaches the reasoning call  | PASS — via `creativeHistory` param + `formatCreativeHistory` block |
| Pattern observations gated by sufficiency           | PASS — thresholds in `PATTERN_SUFFICIENCY`, echoed in prompt |
| v1 plain-text frontend contract preserved           | PASS — structuring call emits v1 format first, sentinel + JSON second |
| Analyst voice on framing text                       | PASS — voice rules in both reasoning and structuring system prompts |
| Creative voice on hooks / copy / visuals / CTAs     | PASS — brand tone system unchanged from v1 |
| `creative_pattern_observations` output field        | PASS — 0 to 3 entries, JSON-serialised in Airtable |
| `forward_preparation` output field                  | PASS — string-or-null |
| Objective-strict, currency discipline, season logic | PASS — preserved bit-exact from v1 |
| Brand tone system preserved                         | PASS — all 8 tones verbatim |
| Three-call architecture                             | PASS — reasoning, structuring, briefs; no new calls |
| Minimum spend threshold                             | PASS — `MIN_SPEND_FOR_ANALYSIS = 50` |
| Graceful degradation on missing deps                | PASS — see §2.5 |

---

## 5. Risks & items for Peach to sign off

### 5.1 Sentinel collision — LOW risk
`===PATTERNS_JSON===` is visually distinct and unlikely to appear in
analyst prose. If an account has a genuinely weird ad name containing that
exact string, the plain-text block would be truncated at the false
sentinel. Acceptable risk; would require that exact 19-character literal
to appear verbatim.

### 5.2 Token budget — MONITOR
Adding 90-day history + pre-computed format aggregations to the data block
increases input tokens on the reasoning call. `HISTORY_MAX_ADS_DEFAULT` is
50 to keep the block lean. Structuring call max_tokens bumped from 2000 →
2500 to accommodate the JSON tail. Reasoning call bumped from 1500 → 1800.
Watch for truncation on accounts with the largest histories; raise
`max_tokens` or lower `HISTORY_MAX_ADS_DEFAULT` if it happens.

### 5.3 D1 dependency — REQUIRES PEACH CONFIRMATION
v2 expects the `BASELINE_DB` binding (same D1 Step 6 v2 uses) and the
`baseline_context` table schema (`account_key`, `week_ending`,
`season_tags`, `sufficiency_4w`, `sufficiency_12w`, `sufficiency_lifetime`,
`sufficiency_seasonal_yoy`). If the schema has drifted since Step 6 v2,
the reads succeed but the fields used will be undefined and sufficiency
flags default to `insufficient`. **Peach: confirm schema matches or flag
the drift.**

### 5.4 Airtable field creation — BLOCKS USEFUL OUTPUT
The worker will run without the two new fields existing — `typecast:true`
will either create them automatically (if the base allows it) or Airtable
will 422 the unknown fields and the rest of the record still writes
(because Airtable API rejects the whole record on unknown field, actually
— **this is the important failure mode to verify on the smoke test**).
Safest path: create the two long-text fields before the first run.

### 5.5 Drift between draft and worker — LOW risk, MONITORED
`step-7-v2-prompts-DRAFT.js` and the inline copies in the worker are
duplicated intentionally. Any future prompt change must be applied to
both. The draft file is the reviewable source; the worker is the
deployed code.

### 5.6 Brand tone default — PRESERVED BEHAVIOUR
Accounts without a `brand_tone` field in Connected_Accounts default to
`'warm'`. Same as v1. Peach may want to audit which accounts are on the
default vs an explicit tone — but this is not a v2 change.

---

## 6. File structure & line counts

```
step-7-peach-system-creative-analysis-v1.js         1109   # unchanged, live v1
step-7-peach-system-creative-analysis-v2.js         1706   # new v2 worker
step-7-v2-prompts-DRAFT.js                           876   # reviewable prompt draft
STEP-7-V2-DESIGN-BRIEF.md                              -   # design reference
CLAUDE-CODE-BRIEF-STEP-7-V2.md                         -   # operational brief
STEP-7-V2-DECISIONS.md                                 -   # this document
```

### Worker module map (`step-7-peach-system-creative-analysis-v2.js`)
- lines 1-118     header + constants (includes `PATTERN_SUFFICIENCY`)
- lines 119-147   entry points (`scheduled`, `fetch`, `/health`, `/run`, `/run-single`)
- lines 148-165   `runSingleCreativeAccount`
- lines 166-234   `runCreativeAnalysis` + `getLastSunday` + `getActiveAccounts`
- lines 236-378   `processAccount` (v2 wiring)
- lines 381-411   `computeFormatAggregations`
- lines 414-459   `parseStructuringResponse`
- lines 461-596   fetches: `getWeeklyCreatives`, `getBaseline`, `getClientProfile`, `getCreativeHistory`, `getBaselineContext`, `getMostCommonObjective`
- lines 599-1335  v2 prompt functions (inlined from draft)
- lines 1340-1403 `callClaude`, `parseBriefResponse`
- lines 1405-1485 `writeResults` (with new fields)
- lines 1487-1706 utilities: Islamic calendar, `getSeasonStatus`, `sanitise`, `airtableGet`, `airtablePatch`, `sleep`

---

## 7. Verification

Both files pass `node --check` at the tip of `claude/step-7-v2-execution-b6INF`:

```
$ node --check step-7-peach-system-creative-analysis-v2.js   # 1706 lines  OK
$ node --check step-7-v2-prompts-DRAFT.js                    #  876 lines  OK
```

Last commit: `ed07793` (B2 worker wiring).
