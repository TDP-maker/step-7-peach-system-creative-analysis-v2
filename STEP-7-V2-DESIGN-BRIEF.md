# Step 7 v2 — Creative Analysis Design Brief

A working document capturing the design decisions for the next iteration of Step 7 (creative analysis + brief generation).

**Purpose:** lock thinking so next session (or Claude Code) can execute from a clear brief rather than inventing on the fly.

**Paired with:** Step 6 v2 architecture (already deployed). Step 7 v2 shares the same principles, data warehouse, and voice discipline — but applies them to creative rather than performance.

**Not in scope yet:** Creative Intelligence Library full schema (Phase 2), concept-testing memory (Phase 2), frontend redesign of creative section (separate project).

---

## 1. Core product principles (locked — same as Step 6)

These drive every prompt and feature decision.

### 1.1 Never forget, always frame
The account's full creative history is available. Intelligence lives in how we frame what's been run, not in what we hide. Every creative's performance is preserved.

### 1.2 Specific over confident. Observational over directive.
For the client-facing text (framing, rationale), present what the data shows. For the creative content itself (hooks, copy), the job is different — see 1.6.

### 1.3 Experienced data analyst (for the framing)
The voice around creative briefs is analyst voice. Not salesy. Not overpromising. The AI explains what patterns the data shows and why this brief direction is proposed.

### 1.4 Retention through visible compounding depth
Every weekly creative report should make the subscriber feel their creative history is becoming richer. Pattern observations that couldn't be made last week are the retention mechanism.

### 1.5 Pattern recognition is the defensible moat
Creative pattern recognition across accumulated history is genuinely hard to do elsewhere. Every creative report should contain at least one observation a subscriber couldn't get by pasting this week's ads into ChatGPT.

### 1.6 Creative voice for creative, analyst voice for framing
**This is the key distinction for Step 7.**

- **Creative voice** governs: hooks, ad copy, headlines, visual concepts, CTAs, subheadlines. Designed to stop the scroll and create connection with the audience. Persuasive by design. Brand tone system applies here (playful, premium, bold, warm, aspirational, cultural, urgent, professional).

- **Analyst voice** governs: `why_it_works`, `format_adaptations`, and any descriptive text the CLIENT reads about the brief. Observational, grounded in data, respects the limits of what the data proves.

The rule: if the text is going INTO a Meta ad, it's creative voice. If it's explaining a brief to the client, it's analyst voice.

---

## 2. What to preserve from Step 7 v1

Step 7 v1 is genuinely strong. Preserve these:

### 2.1 Three-call architecture
Reasoning → Structure → Briefs. Clean separation of thinking, analysis, and creative generation. Keep.

### 2.2 Brand tone system
Eight distinct tones (premium, playful, bold, professional, warm, aspirational, cultural, urgent) with specific hook and copy rules per tone. This is one of the strongest pieces of craft in the whole system. Preserve entirely.

### 2.3 Objective-strict handling
Lead-gen accounts don't get ROAS metrics. Sales accounts don't get lead commentary. Traffic accounts don't get conversion talk. Preserve and extend to creative pattern analysis.

### 2.4 Currency discipline
Numbers only, never £/$/€/﷼ symbols. Preserve.

### 2.5 Season status logic
Upcoming / active / recently_ended / past handling with Islamic calendar calculation. Genuinely good. Preserve.

### 2.6 Minimum spend threshold for judgement
50 currency units minimum before an ad is judged. Ads below threshold go into "Insufficient data" section. Preserve.

### 2.7 Fatigue flag awareness
When fatigue_flag is set, the ad is flagged regardless of current week's numbers. Preserve.

### 2.8 Hook style variety
v1's brief generation covers aspirational, occasion/urgency, wordplay/puns, identity, and social proof/curiosity hook styles with examples per brand tone. Preserve the variety — extend it if anything.

### 2.9 Format adaptations guidance
Feed vs Stories/Reels reframing instructions in every brief. Preserve.

### 2.10 British English, no em dashes in framing text
Voice discipline already in place. Preserve.

---

## 3. What to fix (gaps in v1)

### 3.1 Creative history blindness
Step 7 v1 only reads this week's 20 creatives. It has no visibility into the account's 90-day creative history. Account-specific patterns are invisible.

**Fix:** add a data fetch that pulls the last 90 days of creatives from `creative_output_table` (or a reasonable subset — see Section 7 for the exact query shape). Pass this history into the reasoning call so the AI can surface patterns.

### 3.2 Framing text drifts salesy
`why_it_works` currently asserts rather than observes. Examples from v1: "proving this theme resonates strongly" overclaims causation.

**Fix:** apply analyst voice to framing text. Same discipline as Step 6 — specific, conditional, hypothesis-plural where causation is uncertain.

### 3.3 Briefs generated from this-week-only data
Current brief generation uses this week's top performers as the basis for new brief directions. A smarter version uses patterns across 90 days of creative performance.

**Fix:** pass creative history into the brief-generation call. Rationale for each brief should reference patterns across history, not just this week's top ad.

### 3.4 No pattern observations in the analysis output
Step 6 has `pattern_observations` as a distinct output field for "wow" moments. Step 7 currently surfaces nothing equivalent for creative.

**Fix:** add a `creative_pattern_observations` output field, populated with genuinely pattern-grounded observations about the account's creative history.

---

## 4. What to add (new capabilities in v2)

### 4.1 Hook style pattern recognition
Cluster hooks across 90 days into styles (occasion-based, identity-based, urgency-based, wordplay, aspirational, feature-led) and identify which styles consistently outperform for this specific account.

**Example observation:**
"Across 24 ads in the last 3 months, your occasion-based hooks averaged 5.8 ROAS. Feature-led hooks averaged 2.1. This week's top performer is occasion-based, which aligns with your account's pattern."

**Implementation note:** Hook clustering can be AI-inferred from hook_text — no explicit taxonomy needed upfront. The reasoning call can identify clusters and report back. For Phase 1 we rely on AI judgement. Phase 2 could formalise taxonomy in the Creative Intelligence Library.

### 4.2 Format-performance pairing
Which format works best for this account across actual performance, not just volume.

**Example observation:**
"Your carousels have outperformed single images by an average of 2.3x ROAS over the last 90 days. This week you ran 3 carousels and 7 static ads — the ratio is skewed toward the weaker format."

**Implementation note:** Pure computation from ad_format + performance data. Can be pre-rendered in the data block as a small aggregated table.

### 4.3 Video retention cliff detection
For accounts with video ads, identify where viewers consistently drop off across the account's videos over 90 days.

**Example observation:**
"Your videos consistently lose 62% of viewers between p25 and p50 retention across your last 18 videos. Hooks are holding attention — mid-video content isn't."

**Implementation note:** Compute average retention curve across account's 90-day video ads. Surface the steepest drop as the cliff. Only include when account has 5+ video ads in history.

### 4.4 Visual theme pattern recognition
From `image_tags`, identify which visual themes consistently win across the account.

**Example observation:**
"Ads tagged 'French pastry' or 'gift box' have averaged 4x the ROAS of ads tagged 'product-only' across your last 40 creatives."

**Implementation note:** AI-inferred clustering of image_tags from the historical data. Similar approach to hook style — let the AI surface the pattern rather than building a formal taxonomy.

### 4.5 Creative pattern observations output field
New top-level output field: `creative_pattern_observations` (array). Mirror of Step 6's `pattern_observations` but for creative insights.

Each observation:
- `title` (short, e.g. "Occasion-based hooks consistently outperform")
- `detail` (2-4 sentences, analyst voice, grounded in specific history)
- `pattern_type` (hook_style | format_performance | video_retention | visual_theme | fatigue_curve | cross_metric)

Rough bar for "notable":
- Pattern holds across 3+ ads with consistent outcome
- Effect size meaningful (2x+ performance difference, or 15%+ shift)
- Only present if 90-day history supports it

### 4.6 Fatigue curve awareness
Use the fatigue_flag history plus performance-over-time to identify the account's typical fatigue timeline.

**Example observation:**
"Your creatives typically fatigue around week 5-6 of sustained spend. Three of your current active ads are in week 4. Brief replacements now to avoid a performance gap."

**Implementation note:** Identify the typical age at which ads flip the fatigue_flag, or at which their weekly ROAS drops below a threshold. Needs at least 10 historical creatives with fatigue data.

### 4.7 Brief rationale grounded in pattern, not this-week top performer
When generating briefs, the `why_it_works` field should reference account-specific patterns, not just "this week's top ad did X".

**Current v1 approach:**
"The French pastry theme carousel produced your strongest ROAS this week at 47.59."

**v2 analyst-voice + pattern-grounded:**
"Occasion-led carousels have consistently been your strongest format across the last 12 weeks, averaging 5.8 ROAS compared to 2.1 for feature-led creative. This brief tests the same pattern with a sharper gifting focus."

### 4.8 Cross-metric creative synthesis (stretch — include if data supports)
Connect creative attributes to funnel behaviour where the data supports it.

**Example observation:**
"Your video ads drive 40% more add-to-carts than your static ads, but your static ads convert ATC-to-purchase at 2x the rate of video. Video is winning attention; static is closing the sale. Consider this split when briefing new creative."

**Implementation note:** Cross-reference creative-level data with weekly funnel rates. Requires joining ad-level performance to funnel flow. Include if the join is straightforward; defer if it requires schema changes.

---

## 5. Proposed Step 7 v2 output structure

### 5.1 Creative analysis (this week's creatives)
Preserved from v1 but with analyst voice applied to "Why" sentences.

- Scale These (up to 3 ads)
- Consider Pausing (up to 3 ads)
- Watch Closely (up to 3 ads)
- Insufficient Data (list)
- Key Insight (one sentence — now pattern-aware where possible)
- Recommended Action (one sentence — specific and immediate)

**Shift from v1:** The "Why" sentence for each ad should now reference patterns where relevant: "Your third French-pastry-themed creative to cross 5x ROAS this quarter. Theme continues to be your strongest."

### 5.2 Creative pattern observations (new in v2)
Standalone section, array of 1-3 pattern observations from the 90-day history. Each with title, detail, pattern_type.

This is the "wow" section. Only populated when 90-day history supports it. For accounts with <4 weeks of data, this section is empty and the prompt handles that honestly.

### 5.3 Creative briefs (three per week, preserved from v1)
Preserved from v1 with two changes:
- `why_it_works` is analyst voice and pattern-grounded
- Brief directions themselves should draw on patterns, not just this week's top performer

All creative fields (hook, ad copy, headline, visual concept, CTA, subheadline, format adaptations) stay in creative voice with brand tone applied.

### 5.4 Forward-preparation (conditional, mirrors Step 6)
One or two sentences when there's a meaningful forward-facing creative observation. Examples:
- "Ramadan is five weeks out — worth briefing seasonal creative now."
- "Your carousel performance pattern is becoming statistically reliable — we'll have enough data to identify optimal carousel card count in 4 more weeks."
- "Your next creative fatigue cycle is expected around week 6 — three current ads are entering that window."

Null when there's nothing forward-facing to say.

### 5.5 Context snapshot (machine-readable, for immutability)
Embed the creative history window, baseline state, and pattern data used at generation time. Mirrors Step 6's approach.

---

## 6. Step 7 v2 voice discipline

Summarising the voice rules in one place:

### Creative content (hooks, copy, headlines, visual concepts, CTAs)
- Follow brand tone system (8 tones with existing rules)
- Persuasive by design
- Written for the audience, not the brand
- Brand must not appear in hook
- Hook under 10 words
- Copy under 125 characters
- No em dashes, numbers only (not currency symbols)

### Analyst-voice framing (why_it_works, format_adaptations, creative_pattern_observations, analysis prose)
- Observational, not directive
- Specific numbers with context
- Hypothesis-plural when causation is uncertain
- Reference account-specific patterns where they exist
- Don't overclaim — "this has consistently performed" not "this will work"
- British English
- No em dashes
- No motivational language
- No unhedged imperatives

### When the two voices meet in a brief
The brief is a single artefact, but it contains both voices. The hook is creative voice. The why_it_works below it is analyst voice. The client should feel: "This brief was written by someone who thought carefully about my data AND knows how to write ads." That duality is the product.

---

## 7. Data requirements for v2 prompts

What each prompt call needs in its context window.

### 7.1 Reasoning call data block

- This week's creatives (same as v1 — top 20 by ROAS + spend)
- **New:** Account's creative history across the last 90 days from `creative_output_table` — aim for roughly the top 50-100 creatives by spend, to keep tokens manageable. Include: ad_name, hook_text, ad_copy, ad_format, image_tags, spend, roas, cpa, purchases, leads, fatigue_flag, video retention percentiles (for videos), week_ending_date
- Connected_Accounts + client_profile (unchanged from v1)
- Brand tone (unchanged from v1)
- **New:** Baseline context from D1 for currency / season / sufficiency state
- **New:** Pre-computed format-performance aggregation (optional — helps the AI reason faster). Small table: format × average_roas × count × lifetime_spend

### 7.2 Structuring call data block
Same as v1 — extracts from reasoning output, doesn't need new data.

### 7.3 Brief generation call data block
- The structured analysis output (unchanged)
- Brand tone, client profile, currency (unchanged)
- **New:** The identified creative patterns from the reasoning call (for brief rationale grounding)

### 7.4 Creative history query specification

```sql
-- conceptually (Airtable-ified)
SELECT ad_name, hook_text, ad_copy, headline, visual_text, image_tags, 
       ad_format, video_duration, video_plays_p25, video_plays_p50, 
       video_plays_p75, video_plays_p100, video_thruplay, fatigue_flag,
       spend, roas, cpa, purchases, leads, clicks, week_ending_date
FROM creative_output_table
WHERE ad_account_id = ?
  AND week_ending_date >= (today - 90 days)
  AND spend >= 50 -- respect the MIN_SPEND_FOR_ANALYSIS threshold
ORDER BY spend DESC
LIMIT 100
```

If this returns sparse results for new or low-spend accounts, Step 7 v2 gracefully degrades — pattern observations empty, brief generation uses this-week-only as v1 did. Same principle as Step 6's sufficiency flags.

---

## 8. Pattern recognition — implementation approach

### 8.1 AI-detected for Phase 1
All patterns (hook style, format, visual theme, retention, fatigue, cross-metric) are AI-detected from the 90-day history in the data block. No pre-compute layer.

### 8.2 Why AI-detected is fine for Phase 1
- Clustering hooks into styles is a pattern-recognition task Claude handles well
- Visual theme clustering same
- Format aggregation is trivial enough to compute inline
- Video retention averaging is simple statistical work

### 8.3 What pre-compute would add (Phase 2)
- Consistency across accounts (all accounts use same hook style taxonomy)
- Faster prompts (less data in context window)
- Formal Creative Intelligence Library schema

For now, accept AI judgement. Tune with live data.

### 8.4 Pattern sufficiency gating
Explicit rules in the prompt:
- Hook style patterns require 10+ historical creatives with hook_text
- Format patterns require 5+ examples of each compared format
- Video retention cliff requires 5+ videos
- Visual theme patterns require 10+ creatives with image_tags
- Fatigue curve requires 10+ creatives with either fatigue_flag=true or sufficient weekly history
- Cross-metric synthesis requires both creative-level and funnel-level data for the same weeks

When insufficient, the AI honestly says so or omits the observation.

---

## 9. Sequencing for implementation

### Phase 1 (next session — Claude Code)
1. Write Step 7 v2 prompts via Claude Code (similar flow to Step 6):
   - Updated reasoning prompt (includes 90-day history, pattern recognition instructions, analyst-voice framing discipline)
   - Updated structuring prompt (adds creative_pattern_observations and forward_preparation fields)
   - Updated brief-generation prompt (pattern-grounded rationale, hooks stay creative voice)

2. Update Step 7 Worker code:
   - Add 90-day creative history fetch
   - Add baseline context fetch from D1 (currency, season, sufficiency)
   - Pass new inputs to prompts
   - Parse new output fields
   - Write new output fields to `ai_creative_analysis_results`

3. Test against Haflaty and one other active account

### Phase 2 (later — separate project)
- Creative Intelligence Library schema design
- Formal hook style / visual theme taxonomies
- Pre-compute layer for patterns
- Concept-testing memory (brief → produced creative → performance tracking)

### Phase 3 (later)
- Frontend redesign to showcase creative patterns
- Pattern-aware brief selection UI
- Visual thumbnails in weekly report

### Not in scope for Step 7 v2
- Any work beyond Worker code + Airtable fields
- New tables or schema
- Integration with onboarding backfill (separate project)

---

## 10. Airtable schema changes needed

Before Step 7 v2 can deploy, two new fields on `ai_creative_analysis_results`:

### 10.1 `creative_pattern_observations`
- Type: Long text
- Written as: JSON-stringified array of `{ title, detail, pattern_type }` objects
- Null when no patterns

### 10.2 `forward_preparation`
- Type: Long text
- Written as: string or null
- Matches Step 6's same field

Neither replaces existing fields. Both are additive.

---

## 11. Acceptance criteria

For Step 7 v2 to be considered ready for deployment:

### Voice discipline
- Hooks, copy, visual concepts stay in creative voice per brand tone
- `why_it_works`, `format_adaptations`, and pattern observations use analyst voice
- No em dashes in any output field
- British English throughout
- No currency symbols

### Creative analysis section
- Scale/Pause/Watch sections present with up to 3 ads each
- "Why" sentences reference patterns from 90-day history where applicable
- Insufficient Data section lists sub-threshold ads
- Key Insight is pattern-aware for accounts with 4+ weeks of history

### Creative pattern observations
- For accounts with 4+ weeks of data and 10+ historical creatives, at least one pattern observation per week
- Observations are specific, grounded in actual history, not invented
- Pattern type correctly categorised
- Accounts with insufficient history get empty array, prompt handles honestly

### Creative briefs
- Three briefs per week, preserved from v1
- Each brief has hook, copy, headline, visual concept, CTA, subheadline, format adaptations, why_it_works
- Hooks follow brand tone rules
- `why_it_works` is pattern-grounded and analyst voice

### Forward preparation
- Conditional — null when nothing meaningful to say
- When populated, specific and forward-facing
- Never generic filler

### Blind review test
Show 3 sample creative reports to someone familiar with the account. They should not be able to tell which were AI-generated vs human-written by a senior creative strategist. This is the real bar.

---

## 12. Decisions locked

For clarity, decisions from today that don't need revisiting:

1. **90-day history window** — not 30, not lifetime
2. **Read from `creative_output_table` Airtable for now** — Creative Intelligence Library is Phase 2
3. **Hooks stay creative voice, framing becomes analyst voice** — the duality is the product
4. **AI-detected patterns for Phase 1** — pre-compute layer is Phase 2
5. **Three-call architecture preserved** — reasoning → structure → briefs
6. **Brand tone system preserved entirely** — one of the strongest pieces of craft
7. **Pattern observations as distinct output field** — not mixed into issues or briefs
8. **Additive Airtable schema changes only** — no breaking changes to existing field IDs
9. **Creative content rules strictly protected** — hook under 10 words, copy under 125 characters, brand never in hook, etc.

---

## 13. Open questions (for next session)

Things that weren't fully settled today and need a decision before drafting prompts:

1. Exact number of historical creatives to include in data block — lean toward 50 top-spend by default, but may want to tune based on token cost
2. Whether to include ad thumbnails / visual descriptions for pattern recognition or rely on image_tags alone
3. Whether `creative_pattern_observations` should be stored per-observation (linked records) or as a single JSON-stringified array on the weekly record — lean toward JSON array for Phase 1 simplicity
4. How to handle accounts with mixed objectives (sales + lead gen) in creative pattern recognition — lean toward objective-strict segmentation within the analysis
5. Whether to add a creative brief "track record" feature (concept testing memory) now or defer to Phase 2 — lean defer

---

## 14. What makes this genuinely different

Not marketing copy — just the honest statement of what Step 7 v2 will do that competitors don't:

1. **Account-specific creative intelligence across 90 days of history** — most tools analyse this week's ads. Yours sees 3 months of patterns.

2. **Hook style pattern recognition** — most tools don't cluster hook styles at all, let alone identify which consistently win for a specific account.

3. **Video retention cliff detection per account** — generic "videos should be under 30 seconds" is everywhere. Account-specific "your videos lose viewers between p25 and p50" is not.

4. **Analyst-voice framing + creative-voice briefs** — the dual voice is genuinely distinctive. Clients get rigorous reasoning AND usable creative.

5. **Fatigue curve per account** — "your creatives typically fatigue at week 5-6" is an observation only possible with the account's accumulated history.

6. **Pattern-grounded brief rationale** — when a brief says "we're testing this because your last 12 occasion-led carousels averaged 5.8 ROAS", that's defensible creative direction. Most brief generation is handwaving about "this week's top ad."

Each one individually is strong. Together they're a moat.
