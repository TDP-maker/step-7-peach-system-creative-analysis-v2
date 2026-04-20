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

// ─── EXPORTS ─────────────────────────────────────────────────────────────────
// Subsequent chunks add: buildCreativeReasoningPrompt,
// buildCreativeStructuringPrompt, buildBriefPrompt.

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
};
