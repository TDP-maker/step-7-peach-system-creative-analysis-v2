# Feature Backlog — Concept Testing Memory

**Status:** Backlog, post-launch Phase 2
**Logged:** 20 April 2026
**Priority:** High-value but not blocking launch

---

## The concept

The AI tracks the outcomes of its own brief recommendations over time. When it generates new briefs, it references the track record of previously-generated briefs that were actually produced and shipped.

### Example output (aspirational)

> "You've produced 4 of our suggested briefs this quarter. The two emphasising occasion framing averaged 4.1 ROAS. The two testing new angles averaged 1.8. We'd lean toward occasion-led concepts this round."

### Why it matters

- Demonstrates the product learning from its own recommendations
- Creates a feedback loop unique to subscribers who stay long enough to see it compound
- No competitor does this because no competitor generates briefs AND tracks production outcomes
- Core retention mechanism — the longer you stay, the smarter your briefs get specifically for your account

---

## What's needed to build it

### 1. Brief-to-creative linkage
Every AI-generated brief needs a unique identifier. When the client produces a creative inspired by that brief, there must be a way to mark the creative as "originated from brief X."

Options:
- Manual tag on the creative by the client when they upload it
- Inferred link based on hook_text / ad_copy matching (fuzzy)
- UTM or naming convention that encodes the brief ID

### 2. Produced brief tracking
A new Airtable field or table tracking:
- brief_id
- account_id
- week_briefed
- was_produced (boolean)
- produced_creative_id (link to creative_output_table)
- production_date

### 3. Performance aggregation
Once a creative linked to a brief has performance data, aggregate:
- Brief concept type
- ROAS delivered
- CPA delivered
- Lifetime spend on that creative
- Fatigue flag triggered y/n

### 4. Step 7 v3 prompt enhancement
Brief generation call receives the track record and references it when proposing new briefs.

---

## Complexity assessment

- **Engineering:** Medium. New Airtable schema, linkage mechanism, aggregation query.
- **Prompt work:** Low. Step 7 already has the architecture; this is an additional data input.
- **UX:** Medium. Needs a way for clients to mark "I produced this brief." Could be manual or automated via hook_text similarity.

---

## When to prioritise

Post-launch. Reasons:

1. Needs existing subscribers with 8+ weeks of brief generation history to be meaningful
2. Benefits compound over time, so launching with it is less impactful than adding it once there's data to power it
3. Fits naturally into Step 7 v3 after Creative Intelligence Library schema is designed

---

## Notes for future design

- Consider whether track record is shown directly in weekly reports, or only feeds brief generation silently
- Could become a premium-tier feature (Full Access only) to drive upgrades
- Pairs well with an "effectiveness score" per past brief concept
- Creates interesting data for a case study once 6+ months in — "here's how the AI's own recommendations have evolved for this account"

---

## Related items to consider together

- Creative Intelligence Library schema (also Phase 2)
- Anomaly detection (Phase 2)
- Seasonal intelligence proactive alerts (Phase 2)

These four Phase 2 items together constitute the "genuinely singular" tier of the product — the features that move it from "strong intelligence tool" to "irreplaceable weekly habit."
