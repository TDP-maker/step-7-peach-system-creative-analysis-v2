/**
 * ============================================================
 * STEP 7 v2 — CREATIVE ANALYSIS + BRIEFS — PROMPT DRAFTS
 *
 * DRAFT FILE. Not wired. Exports the four updated prompt builders
 * (buildAnalysisPrompt, buildCreativeReasoningPrompt,
 * buildCreativeStructuringPrompt, buildBriefPrompt) plus the helper
 * formatters they depend on. The v2 worker
 * (step-7-peach-system-creative-analysis-v2.js) imports the same
 * shape in its inline copies — this file is the reviewable source
 * of truth for the prompt changes.
 *
 * What changes from v1:
 *   1. 90-day creative history block passed into the reasoning call.
 *   2. Baseline context (currency / season / sufficiency) from D1.
 *   3. Reasoning call gains pattern-recognition instructions + sufficiency gates.
 *   4. Structuring call emits the v1 plain-text report UNCHANGED,
 *      followed by a sentinel line and a JSON tail carrying the two
 *      new output fields (creative_pattern_observations,
 *      forward_preparation). The worker splits on the sentinel so the
 *      frontend contract for creative_analysis is preserved exactly.
 *   5. Brief generation's why_it_works shifts to analyst voice and is
 *      pattern-grounded. Hooks, copy, headlines, visual concepts, CTAs,
 *      subheadlines remain in creative voice — brand tone system
 *      preserved bit-exact from v1.
 *
 * SENTINEL: the structuring call output is split on the literal line
 *   ===PATTERNS_JSON===
 * Everything before the sentinel is the plain-text creative_analysis
 * (written to the existing Airtable field). Everything after is parsed
 * as JSON for the new output fields.
 * ============================================================
 */

// ─── CONSTANTS ───────────────────────────────────────────────────────────────

// Delimiter the structuring call emits between the plain-text analysis
// (for creative_analysis) and the JSON tail carrying the new fields.
// Kept intentionally visually distinct so it cannot collide with anything
// an analyst might plausibly write in the prose above it.
const PATTERNS_JSON_DELIMITER = '===PATTERNS_JSON===';

// Minimum spend before an ad is judged. Mirrors v1. Repeated here so the
// prompt file can render thresholds without taking a dependency on the
// worker.
const MIN_SPEND_FOR_ANALYSIS = 50;

// Creative history window — locked in Design Brief §12.1.
const HISTORY_WINDOW_DAYS = 90;

// Max historical ads to include in the data block. Design Brief §7.1 says
// "roughly 50-100". 50 is the default to keep the token budget lean; the
// worker may pass a larger slice for accounts with unusually long history.
const HISTORY_MAX_ADS_DEFAULT = 50;

// Pattern sufficiency gates, per Design Brief §8.4. The structuring prompt
// quotes these back to the model so the gates are enforced in the output,
// not just in the worker.
const PATTERN_SUFFICIENCY = {
  hookStyleMinAds:     10, // creatives with hook_text
  formatMinPerGroup:    5, // per format compared
  videoRetentionMin:    5, // video creatives in history
  visualThemeMinAds:   10, // creatives with image_tags
  fatigueCurveMinAds:  10, // creatives with fatigue data
};

// ─── SANITISE ────────────────────────────────────────────────────────────────

// Strip lone surrogates so Airtable / JSON serialisation cannot choke.
// Duplicated from the worker so this draft file is standalone-reviewable.
function sanitise(str) {
  if (!str) return '';
  return String(str).replace(/[\uD800-\uDFFF]/g, '');
}

// ─── formatCreativeHistory ───────────────────────────────────────────────────
//
// Renders the 90-day creative history block for the reasoning call.
//
// Input: `history` is an array of Airtable records from
// creative_output_table, each with a `.fields` bag. Already filtered by the
// worker to (a) the target ad_account_id, (b) week_ending_date within
// HISTORY_WINDOW_DAYS, (c) spend >= MIN_SPEND_FOR_ANALYSIS, and sorted by
// spend descending. The worker caps length before calling this.
//
// Output shape is dense-but-scannable: one ad per block, separated by a
// newline. Hook text and image tags are truncated because the AI only
// needs enough to cluster patterns, not full copy. Video retention
// percentiles are emitted compactly on a single line. Fatigue flag is
// called out explicitly when present.
//
// Design Brief §7.1 specifies the fields. Order is chosen so the AI sees
// performance numbers first (the signal), then the creative attributes
// (the variables it clusters on).
//
function formatCreativeHistory(history, currency) {
  if (!Array.isArray(history) || history.length === 0) {
    return 'CREATIVE HISTORY (last ' + HISTORY_WINDOW_DAYS + ' days): none above the ' +
           MIN_SPEND_FOR_ANALYSIS + ' ' + currency + ' spend threshold.';
  }

  const header = 'CREATIVE HISTORY — last ' + HISTORY_WINDOW_DAYS + ' days, ' +
                 history.length + ' ad(s) at or above ' + MIN_SPEND_FOR_ANALYSIS +
                 ' ' + currency + ' spend, sorted by spend descending:';

  const blocks = history.map((rec, i) => {
    const f       = rec.fields || {};
    const spend   = parseFloat(f.spend) || 0;
    const roas    = parseFloat(f.roas);
    const cpa     = parseFloat(f.cpl);
    const format  = f.ad_format || 'unknown';
    const isVideo = ['reel', 'video', 'story'].includes(format);

    const week    = f.week_ending_date || f.week_ending || 'unknown';
    const name    = sanitise(f.ad_name || 'Unnamed').slice(0, 80);

    const lines = [
      'H' + (i + 1) + ': ' + name + '  [week ' + week + ']',
      '  format=' + format +
        ' | spend=' + spend + ' ' + currency +
        ' | roas=' + (isNaN(roas) ? 'n/a' : roas) +
        ' | cpl=' + (isNaN(cpa) ? 'n/a' : cpa) +
        ' | purchases=' + (f.purchases ?? 'n/a') +
        ' | leads=' + (f.leads ?? 'n/a'),
    ];

    if (f.hook_text) {
      lines.push('  hook: ' + sanitise(f.hook_text).slice(0, 120));
    }
    if (f.image_tags) {
      lines.push('  tags: ' + sanitise(f.image_tags).slice(0, 120));
    }
    if (f.fatigue_flag) {
      lines.push('  fatigue_flag: true');
    }
    if (isVideo && f.video_plays_p25 != null) {
      lines.push(
        '  retention: p25=' + (f.video_plays_p25 ?? 'n/a') +
        ' p50=' + (f.video_plays_p50 ?? 'n/a') +
        ' p75=' + (f.video_plays_p75 ?? 'n/a') +
        ' p100=' + (f.video_plays_p100 ?? 'n/a') +
        ' thruplay=' + (f.video_thruplay ?? 'n/a') +
        (f.video_duration ? ' dur=' + f.video_duration + 's' : '')
      );
    }

    return lines.join('\n');
  });

  return header + '\n\n' + blocks.join('\n\n');
}

// ─── formatFormatAggregations ────────────────────────────────────────────────
//
// Pre-computed format × performance aggregation (Design Brief §7.1 optional
// "small table"). The worker computes this inline from the 90-day history
// and passes the rows here. We keep it pre-computed because format
// aggregation is trivially numerical — no reason to burn the AI's tokens
// on arithmetic it cannot verify.
//
// Input shape:
//   [{ format: 'carousel',   count: 12, lifetime_spend: 4200, avg_roas: 5.8 },
//    { format: 'single_image', count: 18, lifetime_spend: 3100, avg_roas: 2.1 },
//    ...]
// Missing rows (e.g. no videos) are simply absent — caller does not need to
// stub them.
//
// Output is a short table block the reasoning call can reference directly.
// The structuring call's format-pattern gating (5+ examples per compared
// format) is reinforced in the prompt itself.
//
function formatFormatAggregations(rows, currency) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return 'FORMAT PERFORMANCE (90-day aggregate): not enough data to compute.';
  }

  const headerLine = 'FORMAT PERFORMANCE — 90-day aggregate (sorted by count):';
  const rowLines = rows
    .slice()
    .sort((a, b) => (b.count || 0) - (a.count || 0))
    .map(r => {
      const fmt   = r.format || 'unknown';
      const count = r.count ?? 0;
      const spend = r.lifetime_spend != null ? r.lifetime_spend : 'n/a';
      const roas  = r.avg_roas != null ? r.avg_roas : 'n/a';
      return '  ' + fmt +
             ': count=' + count +
             ' | spend=' + spend + ' ' + currency +
             ' | avg_roas=' + roas;
    });

  const footer =
    'NOTE: format comparisons require at least ' +
    PATTERN_SUFFICIENCY.formatMinPerGroup +
    ' examples of each compared format before a pattern observation is defensible.';

  return headerLine + '\n' + rowLines.join('\n') + '\n' + footer;
}

// ─── formatCreative ──────────────────────────────────────────────────────────
//
// Renders one of this-week's ads as a block for the data-block passed to the
// reasoning call. Structurally identical to v1's formatter — shape is
// preserved so the reasoning call reads the same familiar layout for
// this-week's creatives. The 90-day history is rendered separately by
// formatCreativeHistory above.
//
function formatCreative(c, currency, index) {
  const f       = c.fields || {};
  const spend   = parseFloat(f.spend) || 0;
  const roas    = parseFloat(f.roas) || 0;
  const cpa     = parseFloat(f.cpl) || 0;
  const isVideo = ['reel', 'video', 'story'].includes(f.ad_format);

  const lines = [
    'AD ' + (index + 1) + ': ' + (f.ad_name || 'Unnamed'),
    'Format: ' + (f.ad_format || 'unknown') +
      ' | Spend: ' + spend + ' ' + currency +
      ' | ROAS: ' + (roas || 'n/a') +
      ' | Purchases: ' + (f.purchases ?? 'n/a') +
      ' | Leads: ' + (f.leads ?? 'n/a') +
      ' | CPL: ' + (cpa || 'n/a'),
  ];

  if (f.hook_text)    lines.push('Hook: '        + sanitise(f.hook_text).slice(0, 150));
  if (f.ad_copy)      lines.push('Copy: '        + sanitise(f.ad_copy).slice(0, 200) +
                                 (sanitise(f.ad_copy).length > 200 ? '...' : ''));
  if (f.headline)     lines.push('Headline: '    + sanitise(f.headline).slice(0, 150));
  if (f.visual_text)  lines.push('Visual text: ' + sanitise(f.visual_text).slice(0, 150));
  if (f.image_tags)   lines.push('Visual tags: ' + sanitise(f.image_tags).slice(0, 200));
  if (f.fatigue_flag) lines.push('FATIGUE FLAG: this ad is showing signs of creative fatigue');

  if (isVideo) {
    if (f.video_duration)      lines.push('Video duration: ' + f.video_duration + 's');
    if (f.video_plays_p25 != null) {
      lines.push(
        'Retention: 25%=' + f.video_plays_p25 +
        ' | 50%='  + (f.video_plays_p50  ?? 'n/a') +
        ' | 75%='  + (f.video_plays_p75  ?? 'n/a') +
        ' | 100%=' + (f.video_plays_p100 ?? 'n/a') +
        ' | Thruplay=' + (f.video_thruplay ?? 'n/a')
      );
    }
  }

  return lines.join('\n');
}

// ─── buildAnalysisPrompt ─────────────────────────────────────────────────────
//
// Assembles the data block passed into the reasoning call (and echoed for
// reference into the structuring call). v1 returned `{ systemPrompt,
// userMessage }` but only userMessage was ever consumed — the rest of the
// three-call architecture does the actual prompt work. v2 simplifies the
// signature to return a single string: the data block itself.
//
// Inputs:
//   - companyName, adAccountId, weekEnding, currency           (account id)
//   - objective, aov, breakEvenROAS, targetCPA                 (client profile)
//   - primaryGeo                                               (client profile)
//   - seasonTags                                               (from baseline)
//   - seasonStatus                                             (upcoming/active/recently_ended/past)
//   - analysable                                               (this week's ads >= MIN spend)
//   - thinData                                                 (this week's ads < MIN spend)
//   - baseline                                                 (Airtable baseline_current row fields)
//   - creativeHistory                                          (90-day array, Design Brief §7.4)
//   - formatAggregations                                       (pre-computed rows, §7.1)
//   - baselineContext                                          (D1 row: sufficiency flags, season)
//
// What v2 adds vs v1:
//   1. CREATIVE HISTORY block (last 90 days, top by spend, >= MIN_SPEND)
//   2. FORMAT PERFORMANCE block (pre-computed aggregate)
//   3. SUFFICIENCY FLAGS block (so the reasoning call knows which windows
//      are safe to reference and which are still being established)
//
function buildAnalysisPrompt({
  companyName, adAccountId, weekEnding, currency,
  objective, aov, breakEvenROAS, targetCPA,
  primaryGeo, seasonTags, seasonStatus,
  analysable, thinData, baseline,
  creativeHistory, formatAggregations, baselineContext,
}) {

  const baselineBlock = baseline ? (
    'ACCOUNT BASELINE (' + companyName + ' normal in ' + currency + '):\n' +
    '- ROAS median: ' + (baseline.roas_4w_median ?? 'n/a') +
      ' | p25: ' + (baseline.roas_4w_p25 ?? 'n/a') +
      ' | p75: ' + (baseline.roas_4w_p75 ?? 'n/a') + '\n' +
    '- CPA median: '  + (baseline.cpa_4w_median ?? 'n/a') + ' ' + currency + '\n' +
    '- CTR median: '  + (baseline.ctr_4w_median ?? 'n/a') + '%\n' +
    '- Trend: '       + (baseline.trend_direction ?? 'unknown')
  ) : 'ACCOUNT BASELINE: not yet available.';

  // D1 sufficiency flags gate which windows the analyst may reference.
  // Mirrors Step 6 v2's approach. When baselineContext is null (D1 miss or
  // binding absent) the reasoning call is told every window is insufficient
  // and defaults to the Airtable baseline above.
  const sufficiency = {
    '4w':           baselineContext?.sufficiency_4w           || 'insufficient',
    '12w':          baselineContext?.sufficiency_12w          || 'insufficient',
    'lifetime':     baselineContext?.sufficiency_lifetime     || 'insufficient',
    'seasonal_yoy': baselineContext?.sufficiency_seasonal_yoy || 'insufficient',
  };
  const sufficiencyBlock =
    'BASELINE SUFFICIENCY (from D1):\n' +
    '- 4-week:              ' + sufficiency['4w'] + '\n' +
    '- 12-week:             ' + sufficiency['12w'] + '\n' +
    '- Lifetime:            ' + sufficiency['lifetime'] + '\n' +
    '- Seasonal YoY:        ' + sufficiency['seasonal_yoy'] + '\n' +
    'Never cite a window flagged "insufficient". When flagged "partial", hedge explicitly.';

  const thisWeekBlocks = (analysable || [])
    .map((c, i) => formatCreative(c, currency, i))
    .join('\n\n---\n\n');

  const thinBlock = (thinData && thinData.length > 0)
    ? '\n\nINSUFFICIENT SPEND (below ' + MIN_SPEND_FOR_ANALYSIS + ' ' + currency + '):\n' +
      thinData.map(c =>
        '- ' + (c.fields.ad_name || 'Unnamed') + ': ' + (c.fields.spend ?? 0) + ' ' + currency + ' spend'
      ).join('\n')
    : '';

  const historyBlock = formatCreativeHistory(creativeHistory || [], currency);
  const formatBlock  = formatFormatAggregations(formatAggregations || [], currency);

  const header =
    'Analyse Meta ad creatives for ' + companyName + ' (' + adAccountId + '):\n\n' +
    'Week ending: ' + weekEnding + '\n' +
    'Currency: ' + currency + '\n' +
    'Objective: ' + objective + '\n' +
    'Geography: ' + primaryGeo + '\n' +
    'Season: '   + seasonTags + ' (status: ' + (seasonStatus || 'unknown') + ')\n' +
    (aov           ? 'AOV: '             + aov           + ' ' + currency + '\n' : '') +
    (breakEvenROAS ? 'Break-even ROAS: ' + breakEvenROAS + '\n' : '') +
    (targetCPA     ? 'Target CPA: '      + targetCPA     + ' ' + currency + '\n' : '');

  return (
    header + '\n' +
    baselineBlock + '\n\n' +
    sufficiencyBlock + '\n\n' +
    historyBlock + '\n\n' +
    formatBlock + '\n\n' +
    'CREATIVES THIS WEEK:\n\n' +
    thisWeekBlocks +
    thinBlock
  );
}

// ─── buildCreativeReasoningPrompt (CALL A) ───────────────────────────────────
//
// Free-form creative reasoning. No JSON. 3 to 5 paragraphs of analyst
// thinking that the structuring call then formats.
//
// v1 gave the model a creative strategist persona and asked "what is going
// on?" against this-week's creatives only. v2 keeps the persona but:
//   1. Tells the model the 90-day CREATIVE HISTORY block exists and must
//      be read before forming any view.
//   2. Names the six pattern types it should look for (hook style, format
//      performance, video retention cliff, visual theme, fatigue curve,
//      cross-metric synthesis) and the sufficiency gates for each.
//   3. Bans cold-start / week-1 framing unless the history is genuinely empty.
//   4. Applies analyst-voice discipline to framing text (British English, no
//      em dashes, no currency symbols, hypothesis-plural when naming causes,
//      observational not directive).
//   5. Bans invented patterns explicitly. An absent pattern is better than
//      a fabricated one.
//
// The user message passes through the data block verbatim. The reasoning
// output is prose — the structuring call (A4) parses it.
//
function buildCreativeReasoningPrompt(dataBlock) {
  return {
    system: `You are a senior Meta ads creative strategist at The Digital Peach, a performance marketing agency working with clients across multiple markets globally, with particular depth in the UAE and wider Gulf region.

Your job right now is to think through one client account's creative performance. No JSON. No headings. 3 to 5 paragraphs of honest, specific analytical thinking. A colleague will structure it afterwards.

## PRODUCT PRINCIPLES — these govern every sentence you write
1. Never forget, always frame. The account's 90-day creative history is in the data block. Intelligence lives in how you frame what has run, not in what you hide.
2. Specific over confident. Observational over directive. Present what the data shows, with numbers and context. Reason conditionally. Never overclaim causation. Never manufacture urgency.
3. Experienced data analyst, speaking to a media buyer and a creative team. Authority comes from understanding the numbers and the craft.
4. Pattern recognition is the moat. The 90-day history is there so you can surface observations a subscriber could not get by pasting this week's ads into a general chatbot. But only when the data actually supports one.

## MARKET AWARENESS
Read the geography field first. Apply market-specific knowledge where it sharpens the reading:
- Gulf region (UAE, Saudi Arabia, Qatar): social proof and urgency culture, WhatsApp-first communication, Arabic/English bilingual audiences, Ramadan/Eid/White Friday seasonality.
- Western markets (UK, US, EU): different trust signals, longer consideration cycles.
- Global accounts: consider which markets may be driving performance.
- If geography is unclear, note it honestly and analyse on what the data shows.

## READ THE 90-DAY HISTORY BEFORE FORMING A VIEW
The data block includes a CREATIVE HISTORY section covering the last ${HISTORY_WINDOW_DAYS} days of this account's ads above the ${MIN_SPEND_FOR_ANALYSIS} currency-unit spend threshold. It also includes a pre-computed FORMAT PERFORMANCE aggregate. Use them. This-week's creatives are not enough on their own to explain what is working for this account.

## PATTERN TYPES TO LOOK FOR — with sufficiency gates
Only surface a pattern if the 90-day history actually supports it. Sufficiency gates you must respect:
- Hook-style pattern (clustering hooks into styles such as occasion-based, identity, urgency, wordplay, aspirational, feature-led, social proof): requires ${PATTERN_SUFFICIENCY.hookStyleMinAds}+ historical creatives with hook_text.
- Format performance (which ad formats win for this account): requires ${PATTERN_SUFFICIENCY.formatMinPerGroup}+ examples of each format being compared.
- Video retention cliff (where viewers typically drop off across the account's videos): requires ${PATTERN_SUFFICIENCY.videoRetentionMin}+ video creatives in history.
- Visual-theme pattern (which image_tags themes recur among winners): requires ${PATTERN_SUFFICIENCY.visualThemeMinAds}+ creatives with image_tags.
- Fatigue curve (typical age at which this account's ads fatigue): requires ${PATTERN_SUFFICIENCY.fatigueCurveMinAds}+ creatives with fatigue_flag or sufficient age data.
- Cross-metric creative synthesis (creative attribute vs funnel behaviour): only when both creative-level and funnel-level data are present for the same weeks.

If a gate is not met, say so honestly in-passage: for example "your creative history is still accumulating — hook-style clusters become measurable at ${PATTERN_SUFFICIENCY.hookStyleMinAds}+ ads, you are at 6 today." Do NOT invent a pattern. An invented pattern is worse than an absent pattern.

## BASELINE SUFFICIENCY
The data block lists sufficiency flags from D1 for the 4-week, 12-week, lifetime, and seasonal year-over-year windows. Never cite a window flagged 'insufficient'. When flagged 'partial', hedge explicitly ("still being established"). Prefer the 4-week window when sufficient; reference 12-week for stability context; reference seasonal YoY only when sufficient AND the week has a meaningful season tag.

## COLD-START LANGUAGE — HARD RULE
Do not use "first week", "new account", "early days", or any equivalent unless the 90-day history in the data block is genuinely empty. If the account has 10 historical creatives but the current week is quiet, it is not a new account — it is a quiet week. Say so honestly. Framing the account as new when it is not is misleading.

## NOTICE, DO NOT SUMMARISE
Do not restate numbers already in the data block. The client can read those. Notice what they mean: streaks (a theme winning repeatedly), breaks (a format that used to win dropping), first-time-ever values, things that contradict the simple reading, things that align across weeks.

## VOICE DISCIPLINE — for the framing / analysis prose you are writing now
This is analyst voice, not creative voice. You are explaining to the client, not writing ad copy.
- British English throughout.
- Never use em dashes (--, —). Use full stops or commas.
- Numbers only. Never currency symbols (£, $, €, ﷼). The currency code is stated in the data block — write "379 QAR" not "£379".
- No motivational language. No sales framing. No unhedged imperatives.
- When you name likely causes, name them plural: "this typically points to A, B, or C". Respect the limits of what the data alone can prove.
- Never say "this proves", "this resonates", or any equivalent causation claim. Say "this has consistently performed", "this is the third occasion-led carousel above 5x ROAS this quarter", or similar observational framing.
- Where it sharpens the reading, tie the observation to the client's specific business ("for a higher-AOV gifting brand like yours...").

## OBJECTIVE-STRICT
If the account's objective is lead generation, evaluate on CPL and lead volume — never on ROAS or purchases. If traffic / clicks, evaluate on CPC and CTR. If sales, evaluate on ROAS, CPA, purchases. Respect the objective in every sentence.

## WHAT TO COVER IN YOUR REASONING
- What genuinely matters this week (one thing, sometimes two — not every ad).
- Which pattern(s), if any, the 90-day history genuinely supports. Cite the specific history entries that support them.
- Which sufficiency gates are not yet met, and what the account is on track to unlock.
- Whether this week's top performers align with or depart from the account's pattern.
- What is worth the client's attention, and what is not.
- Specifically what is worth briefing next, and why, grounded in the account's own history.

Produce only the 3 to 5 paragraphs of analytical thinking. No headings, no bullet lists, no JSON. Write as if briefing a colleague who will do the structured write-up after you.`,
    user: dataBlock +
      '\n\nThink through this account now. Read the 90-day history before forming a view. Notice, do not summarise. Only surface patterns the history actually supports.',
  };
}

// ─── buildCreativeStructuringPrompt (CALL B) ─────────────────────────────────
//
// Takes the reasoning call's prose and produces the combined output the
// worker writes to Airtable:
//
//   [plain-text Scale / Consider Pausing / Watch Closely / Insufficient Data
//    / Key Insight / Recommended Action — v1 format preserved bit-exact]
//   ===PATTERNS_JSON===
//   {"creative_pattern_observations": [...], "forward_preparation": "..." | null}
//
// The worker splits on the sentinel line. Everything before → existing
// creative_analysis Airtable field (unchanged frontend contract). Everything
// after → parsed as JSON for the two new fields.
//
// v1 contract preserved:
//   - Section headers: SCALE THESE / CONSIDER PAUSING / WATCH CLOSELY /
//     INSUFFICIENT DATA / KEY INSIGHT / RECOMMENDED ACTION
//   - Up to 3 ads per section, "Spend: X | <metric>" line, "Why: ..." line
//   - Metric string chosen by objective (isLeadGen / isSales / traffic)
//
// v2 additions:
//   - "Why" sentences are analyst voice and reference the 90-day history
//     where applicable
//   - Appended sentinel + JSON tail with creative_pattern_observations
//     (0-3 entries, each gated by sufficiency) and forward_preparation
//     (string or null)
//
// Params:
//   currency, isLeadGen, isSales, minSpend      (metric + threshold framing)
//   sufficiency                                 (4w/12w/lifetime/seasonal_yoy flags)
//   historyCount                                (number of 90-day ads passed in)
//   seasonTags, seasonStatus                    (from D1 / baseline)
//
function buildCreativeStructuringPrompt(reasoning, dataBlock, params) {
  const {
    currency,
    isLeadGen,
    isSales,
    minSpend,
    sufficiency = {},
    historyCount = 0,
    seasonTags = '',
    seasonStatus = '',
  } = params;

  const metric = isSales
    ? 'ROAS: X | Purchases: X'
    : isLeadGen
      ? 'Leads: X | CPL: X'
      : 'Clicks: X | CPC: X';

  // Pattern observations are only defensible if the 90-day history is deep
  // enough. Gate in the prompt so an honest "not yet" is preferred over a
  // fabricated pattern. Gate thresholds come from PATTERN_SUFFICIENCY.
  const historyThin = historyCount < PATTERN_SUFFICIENCY.hookStyleMinAds;

  const sufficiencyLine =
    '4w=' + (sufficiency['4w'] || 'unknown') +
    ', 12w=' + (sufficiency['12w'] || 'unknown') +
    ', lifetime=' + (sufficiency['lifetime'] || 'unknown') +
    ', seasonal_yoy=' + (sufficiency['seasonal_yoy'] || 'unknown');

  const systemString = `You are structuring a senior creative strategist's analysis into a specific format. The reasoning has already been done — extract the findings and present them clearly. Do not add new conclusions. Do not pad. Preserve the honesty and specificity of the original thinking.

## VOICE — analyst voice on framing text
Every "Why" line, the Key Insight, the Recommended Action, and every pattern observation must be analyst voice:
- Observational, not directive.
- Specific numbers with context.
- Hypothesis-plural when causation is uncertain ("this typically points to A, B, or C").
- "Has consistently performed" not "proves X resonates".
- Reference 90-day history patterns where relevant (a theme, a format, a hook style that the account has repeatedly seen).
- British English throughout.
- Never use em dashes (--, —). Use full stops or commas.
- Numbers only. Never £, $, €, ﷼ or any currency symbol. Write "379 ${currency}", not "£379".
- No motivational language. No sales framing. No unhedged imperatives.

## SUFFICIENCY AWARENESS
Baseline sufficiency this week: ${sufficiencyLine}.
Creative history depth: ${historyCount} ad(s) at or above the ${minSpend} ${currency} spend threshold in the last ${HISTORY_WINDOW_DAYS} days.
- Never cite a baseline window flagged 'insufficient'. Hedge when flagged 'partial'.
- ${historyThin
    ? 'Creative history is below the ' + PATTERN_SUFFICIENCY.hookStyleMinAds +
      '-ad threshold for hook-style and visual-theme patterns. Most pattern observations should be omitted this week. An honest "not yet" is preferred over a fabricated pattern.'
    : 'Creative history is deep enough that at least one pattern observation should be surfaced — but only if the 90-day history in the reasoning genuinely supports one.'}

## COLD-START LANGUAGE — HARD RULE
Do not use "first week", "new account", "early days", or any equivalent unless the 90-day history is genuinely empty. An account with ${historyCount} historical creatives is not new; a quiet current week is not the same as a new account.

## FEW-SHOT EXAMPLES — analyst voice

SCALE THESE example (pattern-aware):
Party Shop destination video
Spend: 420 | ROAS: 11.4 | Purchases: 9
Why: Third occasion-led video above 8x ROAS this quarter, extending a pattern the account has seen repeatedly across gifting-moment campaigns. Hook retention at 38 per cent is above your 4-week p75.

CONSIDER PAUSING example (specific, non-causation):
Laduree still
Spend: 130 | ROAS: 1.2 | Purchases: 1
Why: Well past the ${minSpend} ${currency} reliable-data threshold with only 1 purchase. At 1.2 ROAS the creative is below your 4-week p25 of 2.8, and reallocation to the current occasion-led winner is a reasonable next step.

WATCH CLOSELY example (hedged):
New UGC clip
Spend: 85 | ROAS: 3.8 | Purchases: 2
Why: Promising at this spend level but 2 purchases is not yet enough to be confident. Another week of data is a sensible next step.

## OUTPUT — TWO PARTS

PART 1: plain text, no markdown, exactly this structure (the frontend parses this):

SCALE THESE

[Ad name]
Spend: X | ${metric}
Why: [Analyst-voice sentence citing a data point, and where supported, the 90-day pattern it belongs to]

[Up to 3 ads]


CONSIDER PAUSING

[Ad name]
Spend: X | ${metric}
Why: [Analyst-voice sentence on why — reference the number and hedge the cause]

[Up to 3 ads]


WATCH CLOSELY

[Ad name]
Spend: X | ${metric}
Why: [Promising but needs more data — be specific, no unhedged imperatives]

[Up to 3 ads — omit entirely if none]


INSUFFICIENT DATA

[Ads below ${minSpend} ${currency} spend — one line each]


KEY INSIGHT

[One analyst-voice sentence. Pattern-aware where the 90-day history supports it. Observational, not directive.]


RECOMMENDED ACTION

[One specific, immediately testable next step. Name the actual ad and the actual action. Present it as a reasonable next step, not a command.]

PART 2: on a new line, emit the sentinel exactly as shown, followed by a single JSON object on the following line(s). No text after the JSON object. No markdown fences.

${PATTERNS_JSON_DELIMITER}
{"creative_pattern_observations":[{"title":"...","detail":"...","pattern_type":"hook_style"}],"forward_preparation":"..."}

## JSON RULES
- creative_pattern_observations: array of 0 to 3 objects. Each object has exactly these three string fields:
    title        — short title (under 10 words)
    detail       — 2 to 4 sentences, analyst voice, grounded in specific history. No em dashes. No currency symbols.
    pattern_type — one of: hook_style, format_performance, video_retention, visual_theme, fatigue_curve, cross_metric
- Surface a pattern only when the 90-day history in the reasoning genuinely supports it. Sufficiency gates:
    hook_style         requires ${PATTERN_SUFFICIENCY.hookStyleMinAds}+ historical creatives with hook_text
    format_performance requires ${PATTERN_SUFFICIENCY.formatMinPerGroup}+ examples of each format compared
    video_retention    requires ${PATTERN_SUFFICIENCY.videoRetentionMin}+ video creatives in history
    visual_theme       requires ${PATTERN_SUFFICIENCY.visualThemeMinAds}+ creatives with image_tags
    fatigue_curve      requires ${PATTERN_SUFFICIENCY.fatigueCurveMinAds}+ creatives with fatigue data
    cross_metric       requires both creative-level and funnel-level data for the same weeks
- If a gate is not met, omit that pattern. Do not fabricate. An invented pattern is worse than an absent pattern.
- forward_preparation: a string or null. Populate only when there is a meaningful forward-facing creative observation — for example an upcoming season worth briefing for, a fatigue cycle about to hit, a pattern about to become trackable. Use null when there is nothing forward-facing to say. Never use it as filler. 1 or 2 sentences.
- Season context this week: tags="${seasonTags || '(none)'}", status="${seasonStatus || 'unknown'}". If status is 'upcoming' and the season is within about six weeks, forward_preparation is a strong candidate.
- JSON only. No comments. No trailing commas. Do not wrap in markdown fences. Nothing after the closing brace.

## EXTRACTION RULES
- Extract only what is in the reasoning. Name actual ads. Every Why references a specific number.
- If the reasoning is quiet on a section (e.g. nothing to scale), write the section header and leave the body empty rather than inventing.
- Do not restate the reasoning verbatim — compress to the Why line per ad.`;

  const userString = 'Here is the strategist\'s thinking:\n\n' +
    reasoning +
    '\n\nData for reference:\n\n' +
    dataBlock +
    '\n\nNow produce PART 1 (plain text per the format above), then on a new line the sentinel ' +
    PATTERNS_JSON_DELIMITER +
    ', then PART 2 (the JSON object) on the line(s) after. No text after the JSON object.';

  return { system: systemString, user: userString };
}

// ─── EXPORTS ─────────────────────────────────────────────────────────────────
// Subsequent chunks add: buildBriefPrompt.

export {
  PATTERNS_JSON_DELIMITER,
  MIN_SPEND_FOR_ANALYSIS,
  HISTORY_WINDOW_DAYS,
  HISTORY_MAX_ADS_DEFAULT,
  PATTERN_SUFFICIENCY,
  sanitise,
  formatCreative,
  formatCreativeHistory,
  formatFormatAggregations,
  buildAnalysisPrompt,
  buildCreativeReasoningPrompt,
  buildCreativeStructuringPrompt,
};
