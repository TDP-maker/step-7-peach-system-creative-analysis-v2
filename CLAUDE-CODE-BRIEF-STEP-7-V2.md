# Claude Code Brief — Step 7 v2 (Prompts + Worker Wiring)

## The task in one sentence

Upgrade Step 7 (creative analysis + brief generation) to use 90-day creative history for pattern recognition, shift framing text to analyst voice while keeping creative content in creative voice, and add creative pattern observations as a new output field — mirroring the architecture of Step 6 v2.

---

## Context

**Product:** The Peach System — Meta advertising intelligence SaaS for UAE / Gulf region clients.

**Where in the pipeline:** Step 7 runs every Tuesday at 8:30am GST (30 minutes after Step 6). It analyses this week's creative performance and generates three creative briefs for next week.

**What changed:** Step 6 v2 is already deployed with multi-window baselines, pattern recognition, and analyst-voice framing. Step 7 v2 brings the equivalent upgrade to creative analysis — 90-day creative history, pattern observations on hooks/formats/retention/themes, analyst-voice rationale on why_it_works, all while preserving v1's excellent creative voice for hooks/copy/visuals.

**What's good in v1 that must be preserved:**
- Three-call architecture (reasoning → structure → briefs)
- Brand tone system (8 tones — playful, premium, bold, professional, warm, aspirational, cultural, urgent) with specific hook and copy rules per tone. **This is the single strongest piece of craft in the Worker. Do not change any of it.**
- Objective-strict handling (lead-gen vs sales vs traffic)
- Currency discipline (numbers only, never symbols)
- Season status logic with Islamic calendar calculation (upcoming / active / recently_ended / past)
- Minimum spend threshold for judgement (50 currency units)
- Fatigue flag awareness
- Insufficient data section for sub-threshold ads
- Hook style variety (aspirational, occasion/urgency, wordplay, identity, curiosity)

**What v1 is missing that v2 adds:**
- 90-day creative history access (v1 only sees this week's 20 ads)
- Pattern observations across hook styles, formats, video retention, visual themes, fatigue curves
- Analyst-voice `why_it_works` grounded in account patterns (not this-week-only)
- `creative_pattern_observations` output field
- `forward_preparation` output field (mirrors Step 6)
- Baseline context from D1 (currency / season / sufficiency)

---

## What's in this repo

| File | Purpose |
|------|---------|
| `STEP-7-V2-DESIGN-BRIEF.md` | **Read this first.** All design decisions locked. |
| `step-7-peach-system-creative-analysis-v1.js` | Current live Step 7 Worker. Contains v1 prompts inside buildCreativeReasoningPrompt, buildCreativeStructuringPrompt, buildBriefPrompt, buildAnalysisPrompt. |
| `peach-d1-intelligence-schema.sql` | D1 schema reference (same D1 that Step 6 v2 uses). |
| `step-6-peach-system-ai-analysis-v2.js` | Step 6 v2 reference — shows the pattern for D1 fetches, sufficiency flags, and pattern_observations output handling. Copy the approach, not the code. |

---

## What to do

### 1. Read the design brief first
Read `STEP-7-V2-DESIGN-BRIEF.md` in full. Key sections:

- Section 1: Six core product principles (governs every decision)
- Section 2: What to preserve from v1 (protect the brand tone system especially)
- Section 3: What to fix
- Section 4: What to add
- Section 5: Proposed output structure
- Section 6: Dual voice discipline (creative voice for ads, analyst voice for framing)
- Section 7: Data requirements
- Section 10: Airtable schema changes
- Section 11: Acceptance criteria

### 2. Produce a draft in two phases

**Phase A: Prompt drafting.** Updated versions of:
- `buildCreativeReasoningPrompt()` — adds 90-day history awareness, pattern recognition instruction, analyst-voice framing rules
- `buildCreativeStructuringPrompt()` — adds creative_pattern_observations output field, shifts "Why" sentences to analyst voice, references patterns where applicable
- `buildBriefPrompt()` — brief generation. **Hooks, copy, headlines, visual concepts, CTAs all STAY in creative voice with brand tone system applied.** Only the `why_it_works` field shifts to analyst voice and becomes pattern-grounded. Brief directions themselves should draw on 90-day patterns where available.
- `buildAnalysisPrompt()` — data block construction, expanded to include 90-day history and baseline context

**Phase B: Worker wiring.** Updates to:
- Add a new `getCreativeHistory()` fetch function — queries `creative_output_table` for last 90 days, top ~50 by spend
- Add `getBaselineContext()` fetch (D1, same pattern as Step 6 v2)
- Update `processAccount()` to call both new fetches in parallel alongside existing fetches
- Pass new inputs to the prompts
- Update the response parsing to extract `creative_pattern_observations` and `forward_preparation`
- Update `writeResults()` to write the two new fields to `ai_creative_analysis_results`

### 3. Critical voice rules (non-negotiable)

**For creative content (hooks, copy, headlines, visual concepts, CTAs, subheadlines):**
- Follow the v1 brand tone system EXACTLY as written. Eight tones, each with its own rules. Do not rewrite, do not simplify, do not add tones.
- Persuasive by design
- Hook under 10 words
- Brand never appears in hook
- Copy under 125 characters
- Wordplay and occasion-based hooks encouraged where brand tone permits
- Numbers only, no currency symbols

**For analyst-voice framing text (`why_it_works`, `format_adaptations`, `creative_pattern_observations`, the analysis prose for Scale/Pause/Watch sections):**
- Observational, not directive
- Specific numbers with context
- Hypothesis-plural when causation uncertain
- Reference 90-day patterns where they exist
- British English, no em dashes, no motivational language, no unhedged imperatives
- "Has consistently performed" not "will work"

**The rule:** if the text is going INTO a Meta ad, it's creative voice. If it's explaining a brief or analysis to the client, it's analyst voice.

### 4. Pattern observation rules

Surface a pattern observation ONLY when the 90-day history actually supports one. Sufficiency gates:
- Hook style patterns: 10+ historical creatives with hook_text
- Format patterns: 5+ examples of each compared format
- Video retention cliff: 5+ video creatives in history
- Visual theme patterns: 10+ creatives with image_tags
- Fatigue curve: 10+ creatives with fatigue_flag data or sufficient weekly age data
- Cross-metric synthesis: both creative-level and funnel-level data for the same weeks

When insufficient, the AI either omits the observation or honestly says "your creative history is still accumulating — hook-style patterns become measurable at 10+ ads, currently at 6."

Do NOT invent patterns. An invented pattern is worse than an absent pattern.

### 5. Pattern-grounded brief rationale

v1's `why_it_works` examples:
> "The CAROUSEL creative with French pastry theme achieved 47.59 ROAS, way above the account baseline of 7.18, proving this theme resonates strongly with the Gulf market."

v2 analyst-voice pattern-grounded equivalent:
> "Occasion-led carousels have consistently been your strongest format across the last 12 weeks, averaging 5.8 ROAS compared to 2.1 for feature-led creative. This brief tests the same pattern with a sharper gifting focus."

Note the difference: v1 asserts causation ("proving this theme resonates"). v2 states the pattern ("has consistently been") and proposes a test ("this brief tests").

### 6. Output format

Produce three files:

**File 1:** `step-7-v2-prompts-DRAFT.js` — all four updated prompt functions plus any helper formatters (e.g., formatCreativeHistory, formatCreativePatterns).

**File 2:** `step-7-peach-system-creative-analysis-v2.js` — complete updated Worker (starts from v1, adds the two new fetches, wires new inputs through, parses and writes new output fields).

**File 3:** `STEP-7-V2-DECISIONS.md` — decisions log capturing:
- Any interpretive calls made (flag them rather than guess)
- Any trade-offs between v1 preservation and v2 enhancement
- Anything that needs Peach's sign-off before deployment
- Pre-deploy checklist (Airtable fields to add, bindings to confirm, etc.)

### 7. Chunking guidance (from lessons with Step 6)

Large single-commit edits time out in Claude Code. Break the work into small commits:

**Phase A — Prompt drafting chunks:**
1. Add constants / helpers for creative history formatting
2. Update `buildAnalysisPrompt` data block only
3. Update `buildCreativeReasoningPrompt` 
4. Update `buildCreativeStructuringPrompt` system prompt (split if needed)
5. Update `buildCreativeStructuringPrompt` user message
6. Update `buildBriefPrompt` — analyst voice on why_it_works, preserve everything else

**Phase B — Worker wiring chunks:**
7. Add `getCreativeHistory()` and `getBaselineContext()` fetches
8. Wire `processAccount()` to call the new fetches and pass to prompts
9. Update response parsing for `creative_pattern_observations` and `forward_preparation`
10. Update `writeResults()` to write new fields
11. Add `/health` version bump to v2

After each commit, stop and report. Do not chain chunks without checking in.

### 8. What NOT to do

- Do not modify the brand tone system in any way. Eight tones, each with their rules, preserve exactly.
- Do not change v1's hook style examples or rules. Extend if anything, never replace.
- Do not reduce the creative content rules (hook under 10 words, brand never in hook, etc.).
- Do not touch Step 6 v2 or Step 5 or any other Worker.
- Do not deploy. Produce the files for Peach to review and deploy.
- Do not open a pull request. Push to the branch and stop.
- If you encounter an interpretive decision not covered in the brief, flag it in the decisions log rather than guess.

---

## Acceptance criteria

### Preservation checks
- Brand tone system bit-exact preserved from v1
- Three-call architecture preserved
- Currency discipline preserved (no £/$/€/﷼ anywhere)
- British English throughout
- No em dashes in framing text
- Objective-strict handling preserved
- Fatigue flag handling preserved
- Season status logic preserved

### Addition checks
- 90-day creative history fetched and passed to reasoning call
- Pattern observations output with sufficiency gating
- `why_it_works` is pattern-grounded and analyst voice (no "proving X resonates" type claims)
- `forward_preparation` output field added (conditional, null when nothing meaningful)
- `creative_pattern_observations` output field added (array, empty when insufficient data)
- Baseline context read from D1

### Voice split check
- Hooks, copy, headlines, visual concepts, CTAs all in creative voice per brand tone
- `why_it_works`, `format_adaptations`, and all analysis prose in analyst voice
- No "first week" / "new account" / "early days" language unless truly week 1

### Worker correctness
- Worker compiles (`node --check`)
- `/health` returns `version: 'v2'`
- Graceful degradation when 90-day history is sparse (new accounts, low-spend accounts)
- New Airtable field writes use `typecast: true` to avoid select-option failures

---

## One-liner to send to Claude Code

```
Read STEP-7-V2-DESIGN-BRIEF.md and CLAUDE-CODE-BRIEF-STEP-7-V2.md in the repo, then execute the task. Chunk aggressively to avoid timeouts — stop and report after every commit. Flag anything uncertain rather than guess.
```
