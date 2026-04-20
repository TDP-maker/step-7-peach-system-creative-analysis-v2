/**
 * ============================================================
 * STEP 6 v2 — PEACH SYSTEM AI PERFORMANCE ANALYSIS
 * Worker name : step-6-peach-system-ai-analysis
 * Cron        : 0 4 * * 3  (Tuesday 8:00am GST / 4:00am UTC)
 * Version     : v2 (wiring in progress)
 *
 * Bindings expected:
 *   - AIRTABLE_TOKEN
 *   - AIRTABLE_BASE_ID
 *   - ANTHROPIC_API_KEY
 *   - BASELINE_DB            (D1 binding for peach-system-performance-baselines,
 *                             same binding Step 3-4 v2 and Step 5 v2 use)
 *
 * Build-up order in this branch:
 *   1. D1 / Airtable fetch helpers (getBaselineContext, getBaselineMetrics,
 *      getHistoricalWeeks, getPriorAnalyses)                       [DONE]
 *   2. Entry points + orchestrators (scheduled, fetch, runAnalysis,
 *      runSingleAccount, getWeeklySummaries, getLastSunday)        [THIS COMMIT]
 *   3. processAccount + Airtable context fetches                   [next]
 *   4. Prompt builders + Claude call                               [next]
 *   5. Parse + write layers                                        [next]
 *   6. v2 prompt integration + new output fields                   [next]
 *
 * Kept as a single file end-state per the brief's "produce
 * step-6-peach-system-ai-analysis-v2.js" instruction.
 * ============================================================
 */

const AIRTABLE_API           = 'https://api.airtable.com/v0';
const ANTHROPIC_API          = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL           = 'claude-sonnet-4-20250514';

const TBL_CONNECTED_ACCOUNTS = 'tblok1PRjLxqfjLnF';
const TBL_WEEKLY_SUMMARY     = 'tbl9gExVR2zPKi5Be';
const TBL_BASELINE_CURRENT   = 'baseline_current';
const TBL_CLIENT_PROFILE     = 'tbl7Q8iV2SZSSp4Wi';
const TBL_ANALYSIS_RESULTS   = 'tbly224mwvoStvgb6';

// ─── PROMPT CONSTANTS ────────────────────────────────────────────────────────

// Metrics we actively reference in the baseline block. Order matters for
// presentation. Kept deliberately small to keep the prompt lean — adding every
// metric × window × stat would bloat the context with noise the AI ignores.
const BASELINE_METRICS_PRIMARY = [
  'roas', 'cpa', 'ctr', 'cpm', 'cpc', 'aov',
  'click_to_atc', 'atc_to_ic', 'ic_to_purchase',
  'spend', 'revenue',
];

// Stats included per metric/window. We drop p50 (== median by definition here)
// and keep the shape described in the brief: median, p25, p75, min, max.
const BASELINE_STATS = ['median', 'p25', 'p75', 'min', 'max'];

// Windows we surface, in the order the AI should prefer them.
const BASELINE_WINDOWS = ['4w', '12w', 'lifetime', 'seasonal_yoy'];

// Human labels for each window — used in prompt rendering only.
const WINDOW_LABELS = {
  '4w':           '4-WEEK (recent trend)',
  '12w':          '12-WEEK (quarterly context)',
  'lifetime':     'LIFETIME (full envelope)',
  'seasonal_yoy': 'SEASONAL YEAR-OVER-YEAR (same-season, prior years)',
};

// Human labels for each metric. Percentage metrics get a trailing % in output.
const METRIC_LABELS = {
  roas:             { label: 'ROAS',                 unit: ''     },
  cpa:              { label: 'CPA',                  unit: 'cur'  },
  ctr:              { label: 'CTR',                  unit: '%'    },
  cpm:              { label: 'CPM',                  unit: 'cur'  },
  cpc:              { label: 'CPC',                  unit: 'cur'  },
  aov:              { label: 'AOV',                  unit: 'cur'  },
  click_to_atc:     { label: 'Click to ATC',         unit: '%'    },
  atc_to_ic:        { label: 'ATC to Checkout',      unit: '%'    },
  ic_to_purchase:   { label: 'Checkout to Purchase', unit: '%'    },
  spend:            { label: 'Weekly spend',         unit: 'cur'  },
  revenue:          { label: 'Weekly revenue',       unit: 'cur'  },
};

// ─── ENTRY POINTS ────────────────────────────────────────────────────────────

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runAnalysis(env));
  },
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', step: 6, version: 'v2', worker: 'step-6-peach-system-ai-analysis' });
    }
    if (url.pathname === '/run') {
      ctx.waitUntil(runAnalysis(env));
      return Response.json({ status: 'started', message: 'AI analysis running in background' });
    }
    if (url.pathname === '/run-single' && request.method === 'POST') {
      const accountId = url.searchParams.get('account_id');
      if (!accountId) {
        return Response.json({ error: 'Missing account_id query param' }, { status: 400 });
      }
      try {
        const result = await runSingleAccount(accountId, env);
        return Response.json({ success: true, result });
      } catch (e) {
        return Response.json({ success: false, error: e.message }, { status: 500 });
      }
    }
    return new Response('Not found', { status: 404 });
  }
};

// ─── SINGLE ACCOUNT ──────────────────────────────────────────────────────────

async function runSingleAccount(accountId, env) {
  const weekEnding = getLastSunday();
  const log = [`[SINGLE] account: ${accountId} week: ${weekEnding}`];

  const summaries = await getWeeklySummaries(weekEnding, env);
  const summary = summaries.find(s => {
    const id = s.fields.ad_account_id || '';
    const norm = id.startsWith('act_') ? id : `act_${id}`;
    const normTarget = accountId.startsWith('act_') ? accountId : `act_${accountId}`;
    return norm === normTarget || id === accountId;
  });

  if (!summary) {
    return { status: 'not_found', message: `No summary found for ${accountId} week ${weekEnding}` };
  }

  const result = await processAccount(summary, weekEnding, env, log);
  console.log(log.join('\n'));
  return { status: result, log };
}

// ─── MAIN ORCHESTRATOR ───────────────────────────────────────────────────────

async function runAnalysis(env) {
  const startTime = Date.now();
  const log = [`[STEP 6] Started at ${new Date().toISOString()}`];

  try {
    const weekEnding = getLastSunday();
    log.push(`Week ending: ${weekEnding}`);

    const summaries = await getWeeklySummaries(weekEnding, env);
    log.push(`Found ${summaries.length} account summaries for week ${weekEnding}`);

    if (summaries.length === 0) {
      log.push('No summaries found — Step 3/4 may not have run yet for this week');
      console.log(log.join('\n'));
      return;
    }

    let processed = 0, skipped = 0, errors = 0;

    for (const summary of summaries) {
      try {
        const result = await processAccount(summary, weekEnding, env, log);
        result === 'skipped' ? skipped++ : processed++;
      } catch (e) {
        errors++;
        log.push(`ERROR [${summary.fields.account_key || summary.fields.ad_account_id}]: ${e.message}`);
      }
      await sleep(500);
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    log.push(`\n[STEP 6] Complete in ${elapsed}s — processed: ${processed}, skipped: ${skipped}, errors: ${errors}`);

  } catch (e) {
    log.push(`[STEP 6] FATAL: ${e.message}`);
  }

  console.log(log.join('\n'));
}

// ─── GET LAST SUNDAY ─────────────────────────────────────────────────────────

function getLastSunday() {
  const now = new Date();
  const day = now.getUTCDay();
  const sunday = new Date(now);
  sunday.setUTCDate(now.getUTCDate() - day);
  return sunday.toISOString().slice(0, 10);
}

// ─── FETCH THIS WEEK'S SUMMARIES ─────────────────────────────────────────────

async function getWeeklySummaries(weekEnding, env) {
  const formula = encodeURIComponent(`IS_SAME({week_ending_date}, DATETIME_PARSE('${weekEnding}'), 'day')`);
  const records = [];
  let offset = null;

  do {
    const offsetParam = offset ? `&offset=${offset}` : '';
    const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_WEEKLY_SUMMARY}?filterByFormula=${formula}${offsetParam}`;
    const res = await airtableGet(url, env);
    records.push(...(res.records || []));
    offset = res.offset || null;
  } while (offset);

  return records;
}

// ─── PROCESS ONE ACCOUNT ─────────────────────────────────────────────────────
//
// Ported verbatim from v1 in this commit. The D1 fetches (baseline_context,
// baseline_metrics, historicalWeeks, priorAnalyses) and the widened prompt
// signatures are wired in a later chunk; this commit only establishes the
// function shape and the v1-equivalent flow so subsequent chunks can apply
// surgical edits rather than rewrites.
//
async function processAccount(summary, weekEnding, env, log) {
  const f           = summary.fields;
  const adAccountId = f.ad_account_id;
  const accountKey  = f.account_key || adAccountId;

  if (!accountKey) {
    log.push('SKIP: no account identifier in summary row');
    return 'skipped';
  }

  const hasMetrics = f.spend || f.account_roas || f.account_ctr || f.leads;
  if (!hasMetrics) {
    log.push(`SKIP [${accountKey}]: no metrics in weekly summary`);
    return 'skipped';
  }

  const [
    baseline,
    account,
    baselineContext,
    baselineMetrics,
    historicalWeeks,
    priorAnalyses,
  ] = await Promise.all([
    getBaseline(accountKey, env),
    getAccountContext(adAccountId, accountKey, env),
    getBaselineContext(accountKey, weekEnding, env),
    getBaselineMetrics(accountKey, weekEnding, env),
    getHistoricalWeeks(accountKey, weekEnding, env),
    getPriorAnalyses(accountKey, weekEnding, env),
  ]);

  await sleep(200);
  const clientProfile = await getClientProfile(account, env);

  const weeksOfHistory = baseline?.fields?.weeks_of_history_available || 0;
  const baselineMode   = weeksOfHistory >= 4 ? '4w'
                       : weeksOfHistory >= 2 ? '2w'
                       : 'None';
  const isColdStart    = weeksOfHistory <= 1;
  const baselineStatus = baseline?.fields?.baseline_status || (isColdStart ? 'building' : 'active');

  const currency = f?.currency
                || account?.fields?.['fldB6EBAu2iEJ8Qpm']
                || account?.fields?.account_currency
                || '';

  const aov      = clientProfile?.fields?.aov_typical || parseFloat(f.account_aov) || null;
  const minSpend = clientProfile?.fields?.min_weekly_spend_for_judgement
                || baseline?.fields?.min_spend_for_judgement_used
                || (aov ? aov * 3 : null)
                || 500;
  const currSpend = parseFloat(f.spend) || 0;
  const belowMin  = currSpend > 0 && currSpend < minSpend;

  if (currSpend === 0 && !f.leads && !f.account_ctr) {
    log.push(`SKIP [${accountKey}]: zero spend and no engagement metrics`);
    return 'skipped';
  }

  log.push(`Analysing [${accountKey}] mode:${baselineMode} weeks:${weeksOfHistory} status:${baselineStatus} spend:${currSpend}${currency}`);

  const dataBlock = buildDataBlock({
    weekly: f,
    account: account?.fields, profile: clientProfile?.fields,
    weekEnding, weeksOfHistory, isColdStart,
    belowMin, minSpend, currency,
    baselineContext, baselineMetrics, historicalWeeks, priorAnalyses,
  });

  // CALL A: Free reasoning
  log.push(`[${accountKey}] Step A: reasoning...`);
  const reasoningPrompt = buildReasoningPrompt(dataBlock);
  const reasoning = await callClaude(reasoningPrompt.system, reasoningPrompt.user, env, 1500);
  if (!reasoning) {
    log.push(`SKIP [${accountKey}]: reasoning call returned empty`);
    return 'skipped';
  }
  log.push(`[${accountKey}] Step A complete (${reasoning.length} chars). Step B: structuring...`);
  await sleep(300);

  // CALL B: Structure into JSON
  const addDaysLocal = (base, n) => {
    const d = new Date(base + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().split('T')[0];
  };
  const followUpActNow   = addDaysLocal(weekEnding, 7);
  const followUpThisWeek = addDaysLocal(weekEnding, 14);

  const structuringPrompt = buildStructuringPrompt(reasoning, dataBlock, {
    currency, followUpActNow, followUpThisWeek,
    isColdStart, weeksOfHistory, baselineStatus,
    sufficiency: {
      '4w':           baselineContext?.sufficiency_4w           || 'insufficient',
      '12w':          baselineContext?.sufficiency_12w          || 'insufficient',
      'lifetime':     baselineContext?.sufficiency_lifetime     || 'insufficient',
      'seasonal_yoy': baselineContext?.sufficiency_seasonal_yoy || 'insufficient',
    },
    seasonTags: baselineContext?.season_tags || '',
  });
  const aiResponse = await callClaude(structuringPrompt.system, structuringPrompt.user, env, 3500);
  if (!aiResponse) {
    log.push(`SKIP [${accountKey}]: structuring call returned empty`);
    return 'skipped';
  }

  const {
    issues,
    summary: aiSummary,
    weekly_headline,
    pattern_observations,
    forward_preparation,
  } = parseAIResponse(aiResponse);
  const runId         = `${accountKey}|${weekEnding}`;
  const airtableRecId = account?.id || null;
  const ctxSnapshot   = buildContextSnapshot(f, baseline?.fields, account?.fields, clientProfile?.fields);

  let written = 0;
  for (const issue of issues) {
    await sleep(200);
    await writeIssueRecord(issue, {
      accountKey, adAccountId, airtableRecId, weekEnding, runId,
      issueKey:        `${accountKey}|${weekEnding}|${issue.issue_type || 'general'}|${written}`,
      baselineMode,    isColdStart,
      fullText:        written === 0 ? (aiSummary || aiResponse) : null,
      weeklyHeadline:  written === 0 ? weekly_headline : null,
      patternObservations: written === 0
        ? (pattern_observations?.length ? JSON.stringify(pattern_observations) : null)
        : null,
      forwardPreparation:  written === 0 ? forward_preparation : null,
      contextSnapshot: written === 0 ? ctxSnapshot : null,
    }, env);
    written++;
  }

  if (written === 0) {
    await writeIssueRecord({}, {
      accountKey, adAccountId, airtableRecId, weekEnding, runId,
      issueKey:        `${accountKey}|${weekEnding}|summary`,
      baselineMode,    isColdStart,
      fullText:        aiSummary || aiResponse,
      weeklyHeadline:  weekly_headline,
      patternObservations: pattern_observations?.length
        ? JSON.stringify(pattern_observations)
        : null,
      forwardPreparation:  forward_preparation,
      contextSnapshot: ctxSnapshot,
    }, env);
    written = 1;
  }

  log.push(`DONE [${accountKey}] — ${written} issue record(s) written`);
  return 'processed';
}

// ─── FETCH BASELINE ──────────────────────────────────────────────────────────

async function getBaseline(accountKey, env) {
  const formula = encodeURIComponent(`{account_key}='${accountKey}'`);
  const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_BASELINE_CURRENT}?filterByFormula=${formula}&maxRecords=1`;
  const res = await airtableGet(url, env);
  return res.records?.[0] || null;
}

// ─── FETCH ACCOUNT CONTEXT ───────────────────────────────────────────────────

async function getAccountContext(adAccountId, accountKey, env) {
  if (!adAccountId) return null;
  const norm  = adAccountId.startsWith('act_') ? adAccountId : `act_${adAccountId}`;
  const plain = adAccountId.startsWith('act_') ? adAccountId.replace('act_', '') : adAccountId;
  const formula = encodeURIComponent(
    `OR({ad_account_id}='${adAccountId}',{ad_account_id}='${norm}',{ad_account_id}='${plain}')`
  );
  const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CONNECTED_ACCOUNTS}?filterByFormula=${formula}&maxRecords=1`;
  const res = await airtableGet(url, env);
  return res.records?.[0] || null;
}

// ─── FETCH CLIENT PROFILE ─────────────────────────────────────────────────────

async function getClientProfile(account, env) {
  if (!account) return null;
  const ids = account.fields['client_profile'];
  if (!ids?.length) return null;
  const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CLIENT_PROFILE}/${ids[0]}`;
  try {
    return await airtableGet(url, env);
  } catch {
    return null;
  }
}

// ─── PRIOR WEEKS' ANALYSIS BLOCK ──────────────────────────────────────────────
//
// Up to the last 2 weeks of analysis_results so the AI can thread continuity.
// Deliberately short: weekly_headline + top observation only. More than this
// has historically encouraged the AI to over-reference its own past notes.
//
function formatPriorWeeksBlock(priorAnalyses) {
  if (!Array.isArray(priorAnalyses) || priorAnalyses.length === 0) {
    return `PRIOR WEEKS' ANALYSIS: none available (this is the first tracked week or prior analyses are missing).`;
  }

  const lines = [
    `PRIOR WEEKS' ANALYSIS — for narrative continuity only:`,
    `(Reference a prior week ONLY if this week's data speaks to it. Do not force continuity.)`,
    '',
  ];

  for (const prior of priorAnalyses.slice(0, 2)) {
    const wk    = prior?.week_ending || '?';
    const head  = prior?.weekly_headline || '(no headline recorded)';
    const obsTitle  = prior?.top_observation_title  || null;
    const obsDetail = prior?.top_observation_detail || null;

    lines.push(`Week ending ${wk}:`);
    lines.push(`  Headline: ${head}`);
    if (obsTitle)  lines.push(`  Top observation: ${obsTitle}`);
    if (obsDetail) lines.push(`  Detail: ${obsDetail}`);
    lines.push('');
  }

  return lines.join('\n').trim();
}

// ─── SHARED NUMERIC FORMATTER ─────────────────────────────────────────────────

function fmt(v) {
  if (v === null || v === undefined || v === '') return 'n/a';
  const n = typeof v === 'number' ? v : parseFloat(v);
  if (Number.isNaN(n)) return String(v);
  // Keep ints clean, otherwise 2 dp.
  return Number.isInteger(n) ? String(n) : n.toFixed(2);
}

// ─── BASELINE BLOCK FORMATTER ─────────────────────────────────────────────────
//
// Takes the flat array of baseline_metrics rows and renders a grouped,
// window-by-window block. Windows flagged insufficient get a short placeholder
// line rather than a full table — that keeps the prompt compact while still
// telling the AI why it cannot use that window.
//
function formatBaselineBlock(baselineMetrics, sufficiency, currency) {
  if (!Array.isArray(baselineMetrics) || baselineMetrics.length === 0) {
    return `MULTI-WINDOW BASELINES: not yet available for this account.`;
  }

  // Group into { window: { metric: { stat: value } } }.
  const grouped = {};
  for (const row of baselineMetrics) {
    if (!row?.metric || !row?.window || !row?.stat) continue;
    grouped[row.window]                           = grouped[row.window] || {};
    grouped[row.window][row.metric]               = grouped[row.window][row.metric] || {};
    grouped[row.window][row.metric][row.stat]     = row.value;
  }

  const lines = [
    `MULTI-WINDOW BASELINES — ${currency || 'account currency'}:`,
    `(use the window whose sufficiency is 'sufficient'; for 'partial' hedge explicitly; never cite 'insufficient')`,
    '',
  ];

  for (const window of BASELINE_WINDOWS) {
    const suffState = sufficiency?.[window] || 'insufficient';
    const label     = WINDOW_LABELS[window];

    if (suffState === 'insufficient' || !grouped[window]) {
      lines.push(`${label} — ${suffState}. Do not cite this window.`);
      lines.push('');
      continue;
    }

    lines.push(`${label} — ${suffState}:`);
    for (const metric of BASELINE_METRICS_PRIMARY) {
      const stats = grouped[window]?.[metric];
      if (!stats) continue;
      const meta     = METRIC_LABELS[metric] || { label: metric, unit: '' };
      const unitStr  = meta.unit === 'cur' ? ` ${currency}`
                    : meta.unit === '%'   ? '%'
                    : '';
      const parts = BASELINE_STATS
        .filter(s => stats[s] != null)
        .map(s => `${s} ${stats[s]}${unitStr}`);
      if (parts.length === 0) continue;
      lines.push(`  ${meta.label.padEnd(22)} ${parts.join(' | ')}`);
    }
    lines.push('');
  }

  return lines.join('\n').trim();
}

// ─── 12-WEEK HISTORICAL TREND ─────────────────────────────────────────────────
//
// Renders the account's last ~12 weeks as a compact table the AI can scan for
// streaks, breaks, and cross-metric synthesis. If fewer weeks exist we render
// what's there — the AI already knows weeks_of_history from the status block.
//
function formatHistoricalTrend(historicalWeeks, currency) {
  if (!Array.isArray(historicalWeeks) || historicalWeeks.length === 0) {
    return `12-WEEK HISTORY: no prior weeks available yet.`;
  }

  // Expect newest-first. Render oldest-first so streaks read naturally.
  const ordered = [...historicalWeeks].reverse();

  const lines = [
    `12-WEEK HISTORY (oldest to newest) — use this for pattern recognition:`,
    `week_ending | spend | revenue | roas | cpa | ctr% | atc_to_ic% | ic_to_purchase%`,
  ];

  for (const w of ordered) {
    lines.push([
      w.week_ending_date ?? w.week_ending ?? '?',
      fmt(w.spend),
      fmt(w.revenue),
      fmt(w.account_roas ?? w.roas),
      fmt(w.account_cpa  ?? w.cpa),
      fmt(w.account_ctr  ?? w.ctr),
      fmt(w.atc_to_ic_rate),
      fmt(w.ic_to_purchase_rate),
    ].join(' | '));
  }

  lines.push('');
  lines.push(
    `Pattern-notable heuristics (not rules, just rough bar):`,
    `- A 4+ week streak within a tight range that then breaks is worth noting.`,
    `- A week-on-week move >15% in a normally stable metric is worth noting.`,
    `- A first-time-ever value (best, worst, first above/below a threshold) is worth noting.`,
    `- Metrics moving in unexpected combinations (e.g. AOV up while CPA flat) are worth noting.`,
    `Only surface patterns that are actually present — do not invent.`,
  );

  return lines.join('\n');
}

// ─── BUILD DATA BLOCK ─────────────────────────────────────────────────────────
//
// v2 signature adds:
//   - baselineContext    — row from D1 baseline_context
//   - baselineMetrics    — array of rows from D1 baseline_metrics
//                          shape: [{ metric, window, stat, value }, ...]
//   - historicalWeeks    — up to 12 rows from D1 weekly_account_summary
//                          (this week + up to 11 prior weeks, newest first)
//   - priorAnalyses      — up to 2 most recent analysis_results records
//                          shape: [{ week_ending, weekly_headline,
//                                    top_observation_title,
//                                    top_observation_detail }, ...]
//
// Old inputs retained so the Worker's existing scaffolding keeps working:
//   weekly, account, profile, weekEnding, weeksOfHistory, isColdStart,
//   belowMin, minSpend, currency
//
// `baselineMode` and v1-style single-window `baseline` are NOT used by v2
// prompts. If a caller still passes them they are ignored.
//
function buildDataBlock({
  weekly,
  account,
  profile,
  weekEnding,
  weeksOfHistory,
  isColdStart,
  belowMin,
  minSpend,
  currency,
  baselineContext,
  baselineMetrics,
  historicalWeeks,
  priorAnalyses,
}) {

  const companyName = account?.company_name || 'this account';

  // Objective handling preserved from v1 (objective-strict discipline).
  const objectiveMixRaw  = weekly?.objective_mix || '';
  const primaryObjective = objectiveMixRaw
    ? objectiveMixRaw.split(',')[0].split(':')[0].trim()
    : 'sales';
  const objective  = primaryObjective || 'sales';
  const isMultiObj = objectiveMixRaw.includes(',');

  // Client profile context (unchanged from v1).
  const aov           = profile?.aov_typical || parseFloat(weekly?.account_aov) || null;
  const targetCPA     = profile?.target_cpa || null;
  const breakEvenROAS = profile?.break_even_roas || null;
  const productType   = Array.isArray(profile?.product_type)
                          ? profile.product_type.join(', ')
                          : (profile?.product_type || 'unknown');
  const purchaseSpeed = profile?.purchase_speed || 'unknown';
  const offerType     = Array.isArray(profile?.offer_type)
                          ? profile.offer_type.join(', ')
                          : (profile?.offer_type || 'unknown');
  const currentGoal   = profile?.current_goal || null;
  const seasonalPeaks = profile?.seasonal_peaks || null;
  const primaryGeo    = profile?.primary_geo || account?.account_timezones || 'unknown';

  // Baseline-context-derived fields.
  const seasonTags     = baselineContext?.season_tags || weekly?.season_tags || 'standard';
  const trend          = baselineContext?.trend_direction || 'stable';
  const baselineStatus = baselineContext?.baseline_status
                       || (isColdStart ? 'seeding' : 'active');

  // Sufficiency flags — drive what the AI may and may not claim.
  const suff = {
    '4w':           baselineContext?.sufficiency_4w           || 'insufficient',
    '12w':          baselineContext?.sufficiency_12w          || 'insufficient',
    'lifetime':     baselineContext?.sufficiency_lifetime     || 'insufficient',
    'seasonal_yoy': baselineContext?.sufficiency_seasonal_yoy || 'insufficient',
  };

  // Week-on-week deltas preserved from v1.
  const wowRoas  = weekly?.wow_roas_change  != null
                    ? `${weekly.wow_roas_change  > 0 ? '+' : ''}${weekly.wow_roas_change}%`  : 'n/a';
  const wowSpend = weekly?.wow_spend_change != null
                    ? `${weekly.wow_spend_change > 0 ? '+' : ''}${weekly.wow_spend_change}%` : 'n/a';

  const baselineBlock   = formatBaselineBlock(baselineMetrics, suff, currency);
  const trendBlock      = formatHistoricalTrend(historicalWeeks, currency);
  const priorWeeksBlock = formatPriorWeeksBlock(priorAnalyses);

  return `Analyse Meta ads for ${companyName}:

CLIENT CONTEXT:
- Currency: ${currency}
- Product type: ${productType}
- Typical AOV: ${aov != null ? `${aov} ${currency}` : 'not set'}
- Target CPA: ${targetCPA != null ? `${targetCPA} ${currency}` : 'not set'}
- Break-even ROAS: ${breakEvenROAS ?? 'not set'}
- Purchase speed: ${purchaseSpeed}
- Offer type: ${offerType}
- Geography: ${primaryGeo}
- Primary objective: ${objective}${isMultiObj ? ` (mixed account — full mix: ${objectiveMixRaw})` : ''}
${currentGoal ? `- Current goal: ${currentGoal}` : ''}

ACCOUNT STATUS:
- Baseline status: ${baselineStatus}
- Weeks of history in pipeline: ${weeksOfHistory}
- Season tag this week: ${seasonTags}
${seasonalPeaks ? `- Client's known peak seasons: ${seasonalPeaks}` : ''}
- Overall trend direction: ${trend}

SUFFICIENCY — which comparison windows you may use this week:
- 4-week baseline:        ${suff['4w']}
- 12-week baseline:       ${suff['12w']}
- Lifetime envelope:      ${suff['lifetime']}
- Seasonal year-over-year: ${suff['seasonal_yoy']}
Rule: never make a comparative claim using a window whose sufficiency is 'insufficient'. If 'partial', hedge explicitly.

THIS WEEK — ${weekEnding}:
Spend:                    ${weekly?.spend ?? 'n/a'} ${currency}
Revenue:                  ${weekly?.revenue ?? 'n/a'} ${currency}
Purchases:                ${weekly?.purchases ?? 'n/a'}
Leads:                    ${weekly?.leads ?? 'n/a'}
ROAS:                     ${weekly?.account_roas ?? 'n/a'}
CPA / CPL:                ${weekly?.account_cpa ?? 'n/a'} ${currency}
CTR:                      ${weekly?.account_ctr ?? 'n/a'}%
CPM:                      ${weekly?.account_cpm ?? 'n/a'} ${currency}
CPC:                      ${weekly?.account_cpc ?? 'n/a'} ${currency}
AOV:                      ${weekly?.account_aov ?? 'n/a'} ${currency}
Add to Cart:              ${weekly?.add_to_cart ?? 'n/a'}
Initiate Checkout:        ${weekly?.initiate_checkout ?? 'n/a'}

FUNNEL CONVERSION RATES (% of people who moved to next step):
Click to Add-to-Cart:     ${weekly?.click_to_atc_rate ?? 'n/a'}%
Add-to-Cart to Checkout:  ${weekly?.atc_to_ic_rate ?? 'n/a'}%
Checkout to Purchase:     ${weekly?.ic_to_purchase_rate ?? 'n/a'}%
Click to Purchase:        ${weekly?.click_to_purchase_rate ?? 'n/a'}%

WEEK-ON-WEEK CHANGES (vs last week):
ROAS change:              ${wowRoas}
Spend change:             ${wowSpend}

Active campaigns:         ${weekly?.active_campaigns_count ?? 'n/a'}
Active ad sets:           ${weekly?.active_adsets_count ?? 'n/a'}
Active ads:               ${weekly?.active_ads_count ?? 'n/a'}

${baselineBlock}

${trendBlock}

${priorWeeksBlock}

SPEND THRESHOLD: minimum ${minSpend} ${currency} before pause/scale recommendations.
${belowMin
  ? `BELOW MINIMUM: spend of ${weekly?.spend ?? 0} ${currency} is below threshold — no pause or scale recommendations.`
  : 'Above minimum — pause and scale recommendations permitted.'}`;
}

// ─── REASONING PROMPT (CALL A) ────────────────────────────────────────────────
//
// Free-form analyst thinking. No JSON. 3-5 paragraphs.
//
// Signature preserved from v1 (single positional `dataBlock`). The system
// prompt now carries the five core product principles, the "notice, don't
// summarise" instruction, sufficiency awareness, and explicit bans on
// cold-start language for accounts past week 1.
//
function buildReasoningPrompt(dataBlock) {
  return {
    system: `You are a senior data analyst at The Digital Peach, a UAE-based performance marketing agency. You work with UAE and Gulf region businesses. You understand the regional market: Ramadan and Eid seasonality, WhatsApp culture, mixed Arabic/English audience, Mother's Day, UAE National Day, and clients who expect a thoughtful advisory relationship.

Your job right now is to think through what is actually happening with one of your client accounts this week. No JSON. No headings. 3 to 5 paragraphs of honest, specific analytical thinking.

## Five product principles — these govern every sentence you write
1. Never forget, always frame. The account's full history is available. Intelligence lives in how you frame the data, not in what you hide.
2. Specific over confident. Observational over directive. Present what the data shows, with numbers and context. Reason conditionally. Never imperative. Never overclaim causation. Never manufacture urgency.
3. Experienced data analyst. You are not a salesperson, coach, guru, or marketing friend. Authority comes from understanding the numbers. Your job is to present findings clearly and let the client decide.
4. Retention through visible compounding depth. The subscriber should feel their data is accumulating into something richer week by week. Surface that depth when it has genuinely grown. Do not pitch it.
5. Pattern recognition is the moat. Every report should contain at least one observation a subscriber could not get by pasting their data into ChatGPT — but only when the data actually supports one. Do not invent patterns.

## Notice, do not summarise
Do not restate the numbers that are already in the data block. The client can read those. Your job is to notice what they mean: streaks that have held or just broken, same-looking values in different contexts, metrics that move together in unexpected ways, first-time-ever values, things that break a consistent pattern. If this week is genuinely quiet and there is nothing to notice, say that honestly — a quiet week is not a failure of analysis.

## Multi-window baselines — use them correctly
The data block gives you four comparison windows with a sufficiency flag on each.
- Default reference is the 4-week window when it is sufficient.
- Reference the 12-week window for stability context ("this fits your quarterly pattern", "a break from your 12-week consistency") when sufficient.
- Reference the lifetime envelope for "your best ever" or "your most unusual" moments when sufficient.
- Reference seasonal year-over-year only when it is sufficient AND the week has a meaningful season tag.
- When 4-week and 12-week disagree, reference both and reason about the difference rather than picking one silently.
- Never cite a window flagged 'insufficient'. If flagged 'partial', hedge explicitly ("still being established").

## Cold-start language — the hard rule
Do not use "first week", "new account", "early days", "we need more data before we can judge", "hold steady while we gather data", or any equivalent phrasing UNLESS the data block shows Weeks of history in pipeline is exactly 1. For any account with 2 or more weeks of history, those phrases are banned regardless of how much baseline data exists. An account with 4 weeks of pipeline data has 4 weeks of pipeline data — reference whatever windows are sufficient and disclose what is still being established. Do not frame the account as new.

## Continuity
The data block may include the last 1 to 2 weeks of analysis. Reference them ONLY if this week's numbers speak to them (a concern has worsened, improved, or persisted; a prediction has played out). Do not force continuity. If there is nothing to thread, do not thread.

## Voice discipline — preserved from v1, non-negotiable
- British English throughout.
- Never use em dashes (--, —). Use full stops or commas.
- Numbers only. Never currency symbols (£, $, €, ﷼). The currency is stated in the data block.
- No motivational language. No sales framing. No unhedged imperatives.
- When you name causes, name them plural: "this typically points to A, B, or C". Respect the limits of what the data alone can prove.
- When you translate a metric into plain English, layer it: the raw number, a plain-language restatement, then a concrete implication. Example: "11.92% cart-to-checkout. That is 12 out of every 100 people who added to cart starting checkout. In other words, 9 out of 10 interested customers are dropping off before paying."
- When you suggest an action, make it specific, immediate, and testable, and present it as a reasonable next step rather than a command.
- Tie observations to the client's specific business where context makes it sharper ("for a higher-AOV product like yours...").

## What to cover in your reasoning
- What genuinely matters this week (one thing, sometimes two — not every metric).
- Which baseline windows support your reading and which are not yet sufficient.
- Any pattern worth noticing across the 12-week history, if one is present.
- Whether continuity with last week's report is relevant here.
- What is worth the client's attention, and what is not.
- What you are confident about and what you are uncertain about.

Produce only the 3 to 5 paragraphs of analytical thinking. No headings, no bullet lists, no JSON. Write as if briefing a colleague who will do the structured write-up after you.`,
    user: `${dataBlock}

Think through this account now. What is actually going on? Notice, do not summarise.`
  };
}

// ─── STRUCTURING PROMPT (CALL B) — STUB ───────────────────────────────────────
//
// Stub only in this commit; full body lands in 3b-iii-3b. The signature
// matches the v2 DRAFT so callers compile cleanly now, but invoking this at
// runtime would produce useless output. Do NOT ship.
//
function buildStructuringPrompt(reasoning, dataBlock, params) {
  const {
    currency,
    followUpActNow,
    followUpThisWeek,
    isColdStart,
    weeksOfHistory,
    baselineStatus,
    sufficiency = {},
    seasonTags = '',
  } = params;

  const maxObservations = isColdStart ? 2 : 4;
  const genuineWeekOne  = weeksOfHistory === 1;

  const systemString = `## LANGUAGE RULES — NON-NEGOTIABLE
- Never use em dashes (--, —) anywhere in your output. Use full stops or commas.
- Write in British English.
- Numbers only. Never include currency symbols. Currency is declared in the data block.
- Every observation's action_detail must read as the analyst speaking directly to the client. Specific, conditional, testable. No sales framing, no motivational language, no unhedged imperatives.

## COLD-START LANGUAGE — HARD RULE
${genuineWeekOne
  ? `This account genuinely has 1 week of history in the pipeline. Early-stage framing is appropriate. Say so honestly, set expectations for what becomes available over the coming weeks, and limit yourself to 2 observations maximum.`
  : `This account has ${weeksOfHistory} week(s) of history in the pipeline. You must NEVER use the phrases "first week", "new account", "early days", "we need more data before we can judge", "hold steady while we gather data", or any equivalent cold-start framing. These phrases are banned. Reference whichever baseline windows are sufficient, and disclose honestly which windows are still being established.`}

## MULTI-WINDOW BASELINE USE
Sufficiency this week: 4w=${sufficiency['4w'] || 'unknown'}, 12w=${sufficiency['12w'] || 'unknown'}, lifetime=${sufficiency['lifetime'] || 'unknown'}, seasonal_yoy=${sufficiency['seasonal_yoy'] || 'unknown'}.
- Default to the 4-week window when it is sufficient.
- Reference 12-week for stability context when sufficient ("fits your quarterly pattern", "a break from your 12-week consistency").
- Reference lifetime for "best ever" / "most unusual" moments when sufficient.
- Reference seasonal year-over-year only when sufficient AND the week has a meaningful season tag.
- When 4-week and 12-week disagree, reference both and note the difference rather than picking one silently.
- Never cite a window flagged 'insufficient'. When flagged 'partial', hedge explicitly.
- At least half of all observations should reference a baseline window explicitly.

## OBSERVATION CATEGORIES — use exactly these four values
- "Performing well"     — genuine strengths, scale opportunities, reliable patterns confirmed.
- "Warrants attention"  — risks, anomalies, genuine concerns the client should look at.
- "Worth understanding" — notable shifts or patterns that are not alarming but are meaningful.
- "Coming up"           — forward-looking seasonal prep, upcoming windows, preparatory notes.

## VALID issue_type VALUES — use exactly these, choose the closest match
Performance:
  "Strong performer ready to scale"
  "ROAS below your typical range"
  "Cost per result rising"
  "Checkout drop-off rate is high"
  "Low add-to-cart rate"
  "Low click-through rate"
  "High cost per click"
  "Spend below minimum for reliable results"
  "Creative needs refreshing"
  "Audience needs broadening"
  "Budget pacing issue"
  "Revenue strong this week"

Pattern (new in v2):
  "Streak continued"
  "Streak broken"
  "First-time observation"
  "Cross-metric pattern"

Data and tracking:
  "Conversion tracking needs checking"
  "Incomplete data this week"
  "Baseline still being established"

Seasonal (forward-looking):
  "Upcoming season — prepare now"
  "Season active — optimise now"
  "Post-season — apply learnings"

Account health:
  "Mixed campaign objectives detected"

(Note: "First week of data" has been removed. If the account is genuinely in week 1, use "Baseline still being established" and set category to "Coming up".)

## PATTERN OBSERVATIONS — the "wow" field
Top-level field: pattern_observations (array). These are observations that could ONLY be made with the account's accumulated history — streaks, breaks, first-time-ever values, cross-metric synthesis, repeat patterns. Surface them only when the 12-week history in the data block actually supports one. An invented pattern is worse than an absent pattern.

Rough bar for "notable":
- A 4+ week streak within a tight range, especially if it just broke.
- A first-time-ever value (best, worst, first above/below a meaningful threshold).
- A cross-metric move that contradicts a simple reading (AOV up while CPA flat means cost per revenue fell, etc.).
- A week-on-week shift of 15% or more in a metric that is usually stable.

Each pattern observation is a short paragraph (2-4 sentences). Do not duplicate something already in the regular issues. Pattern observations live alongside issues, not inside them.

For accounts with 4+ weeks of history, aim for at least one genuine pattern observation when the data supports it. Do not force one.

## FORWARD PREPARATION — conditional, not always present
Top-level field: forward_preparation (string or null). Include ONLY when there is something meaningful to say about what becomes available next — for example a window about to become sufficient, an upcoming seasonal unlock, or a pattern that is about to become trackable. If there is nothing forward-facing to say this week, set this field to null. Never use it as filler.

## SEASONAL REMINDER RULES
Season tags this week: ${seasonTags || '(none provided)'}.
If the data block indicates an upcoming or active relevant season (Ramadan, Eid, Mother's Day, White Friday, UAE National Day, Back to School, etc.) within six weeks:
- Add ONE seasonal observation in addition to the performance observations.
- Category = "Coming up".
- issue_type = "Upcoming season — prepare now" OR "Season active — optimise now".
- Severity = "Low".
- action_detail forward-looking: what to prepare given the season and the client's product.
- Follow the same voice rules as every other observation.

## WRITING TECHNIQUES TO PRESERVE FROM v1

### Multi-layer number translation
When a rate metric matters, translate it through progressively more intuitive framings:
raw number → plain-language restatement → concrete human implication.
Example: "11.92% cart-to-checkout. Only 12 out of every 100 people who added to cart started checkout. That means 9 in 10 interested customers are dropping off before paying."

### Hypothesis-plural causes
When naming likely causes, offer plural rather than singular: "this typically indicates pricing shock at cart, unexpected shipping costs, or checkout friction". Respect the limits of what the data alone can prove.

### Business-context framing
Where relevant, tie the observation to the client's specific business ("for a higher-AOV product like yours, this usually points to...").

### Concrete, immediate tasks
Recommendations should be specific, immediate, and testable: "have someone walk through the checkout on mobile this afternoon and note where it stalls". Not "consider optimising the funnel".

## FEW-SHOT EXAMPLES — analyst voice

EXAMPLE A — healthy week, restraint demonstrated, references multiple windows
{
  "issue_category": "Performing well",
  "issue_type": "Revenue strong this week",
  "severity": "Low",
  "confidence": "High",
  "confidence_reason": "ROAS of 7.31 this week, compared against a 4-week median of 6.77 and a 12-week median of 6.4, both flagged sufficient.",
  "primary_metric_name": "ROAS",
  "primary_metric_value": 7.31,
  "action_detail": "ROAS came in at 7.31, above both your 4-week median of 6.77 and your 12-week median of 6.4. Spend held flat, so the move was driven by revenue rather than efficiency shifting. This is a steady, honest strong week rather than something volatile. No action is required. If you were already considering a modest scale test, this is a reasonable week to run one, but the data does not demand it."
}

EXAMPLE B — pattern awareness, streak break, multi-window reasoning
{
  "issue_category": "Warrants attention",
  "issue_type": "Streak broken",
  "severity": "High",
  "confidence": "High",
  "confidence_reason": "Cart-to-checkout held between 17 and 20 per cent for the previous three weeks. This week: 11.92 per cent. First break in that range.",
  "primary_metric_name": "Add-to-Cart to Checkout rate",
  "primary_metric_value": 11.92,
  "action_detail": "Your cart-to-checkout dropped to 11.92 per cent this week, the first move outside the 17 to 20 per cent range you have held for the last three weeks. That is 12 out of every 100 people who added to cart starting checkout. In other words, roughly 9 in 10 interested customers are dropping off before paying. For a higher-AOV gifting product like yours, this typically points to unexpected shipping costs at cart, a payment method issue, or something that broke in the mobile checkout flow. Worth having someone walk the cart-to-checkout journey on mobile today and note where it stalls. If the flow is clean, the next place to look is any shipping or fee line that appears at cart rather than at checkout."
}

EXAMPLE C — sufficiency disclosure, hedged honestly, no cold-start language
{
  "issue_category": "Worth understanding",
  "issue_type": "Baseline still being established",
  "severity": "Low",
  "confidence": "Medium",
  "confidence_reason": "4-week baseline is sufficient. 12-week and seasonal year-over-year are still being built. Claims are scoped to the 4-week window.",
  "primary_metric_name": "Weeks of history",
  "primary_metric_value": 4,
  "action_detail": "Your 4-week comparison is now reliable enough to reference, which is why this report cites it directly. Your 12-week picture is still settling and your seasonal year-over-year picture needs a return of the same season in the data before it unlocks. That does not limit what can be said about this week, it just scopes the comparisons to the window where the data genuinely supports them."
}

EXAMPLE D — seasonal, forward-looking, specific preparation
{
  "issue_category": "Coming up",
  "issue_type": "Upcoming season — prepare now",
  "severity": "Low",
  "confidence": "Medium",
  "confidence_reason": "Ramadan is approximately five weeks away and the client's product category has historically performed well in gifting occasions.",
  "primary_metric_name": "Weeks to Ramadan",
  "primary_metric_value": 5,
  "action_detail": "Ramadan is roughly five weeks out, and for a gifting-adjacent product like yours this is typically a strong window. If creative is not yet in preparation, now is a sensible time to brief it — hooks that lean on occasion framing, landing page copy that reflects the moment, any bundle offers that fit the category. Campaigns usually need 7 to 10 days to stabilise after launch, so starting the brief this week leaves room to test before the peak."
}

## OUTPUT FORMAT — valid JSON only, no markdown fences
{
  "weekly_headline": "One sentence, plain English, most important thing this week. Start with a specific number or specific comparison. No em dashes. Example: 'Your ROAS of 7.31 sits in the top quartile of your 4-week range, while your cart-to-checkout dropped to its lowest in the same period.'",
  "forward_preparation": "One or two sentences, or null. Only populate when there is a meaningful forward-facing observation (a window becoming sufficient, an upcoming seasonal unlock, a pattern about to become trackable). Do not include filler.",
  "summary": "2-3 sentences. The opening paragraph the client reads first. Acknowledge what happened this week honestly. Reference actual numbers and at least one baseline window when possible. End with a sentence that sets up the observations below.",
  "pattern_observations": [
    {
      "title": "Short title for the pattern (e.g. 'Third straight week above your 12-week ROAS median').",
      "detail": "2-4 sentences describing the pattern, what makes it notable, and what it does or does not imply. Analyst voice. Only include patterns genuinely supported by the 12-week history."
    }
  ],
  "issues": [
    {
      "issue_category": "Performing well|Warrants attention|Worth understanding|Coming up",
      "issue_type": "Use exact values from the valid list above",
      "severity": "High|Medium|Low",
      "confidence": "High|Medium|Low",
      "confidence_reason": "One sentence citing specific data and the baseline window(s) that support the reading.",
      "primary_metric_name": "The single most important metric for this observation",
      "primary_metric_value": 0.0,
      "primary_metric_change_pct": null,
      "secondary_metric_name": null,
      "secondary_metric_value": null,
      "secondary_metric_change_pct": null,
      "action_type": "Pause|Scale|Refresh creative|Test new angle|Broaden audience|Tighten audience|Fix landing page|Fix checkout|Verify tracking|Hold|Gather more data|Prepare seasonal creative",
      "action_detail": "2-4 sentences. Conversational and direct. Multi-layer translation where a rate metric matters. Hypothesis-plural when naming causes. Business-context framing where it sharpens the point. Specific, immediate, testable recommendation presented as a reasonable next step, not a command.",
      "owner": "Media buyer|Creative team|Landing page|Client",
      "priority": "Act now|This week|Monitor",
      "priority_score": 8,
      "impact_type": "Save spend|Increase revenue|Improve ROAS|Increase CVR|Reduce CPA|Seasonal opportunity",
      "impact_estimate_value": null,
      "impact_estimate_unit": null,
      "follow_up_date": "${followUpActNow}",
      "success_metric": "Specific metric to measure against",
      "success_target": "Target value with units"
    }
  ]
}

RULES:
- Maximum ${maxObservations} observations in the issues array. ${isColdStart ? 'This account is in its first week of data — 2 observations is the cap.' : 'Aim for 2 to 3 sharp observations rather than padding to the cap.'}
- Each issue in the issues array must have a genuinely distinct root cause.
- pattern_observations is a separate top-level array. ${genuineWeekOne
    ? 'For a genuine week-1 account, pattern_observations should be an empty array.'
    : 'Include 0 to 2 pattern observations, only where the 12-week history genuinely supports them.'}
- forward_preparation is a string or null. Use null when there is nothing meaningful to say forward-facing.
- weekly_headline must be one clean sentence with no em dashes and no currency symbols.
- At least half of observations should explicitly reference a sufficient baseline window.
- Never cite a window flagged 'insufficient'.
- priority_score: integer 1-10. Act now = 7-10, This week = 4-6, Monitor = 1-3.
- follow_up_date: Act now = ${followUpActNow}, This week = ${followUpThisWeek}, Monitor = null.
- Seasonal observations always get priority = "This week", priority_score = 5, severity = "Low".
- British English throughout. Numbers only, no currency symbols.`;
  const userString = `Here is the senior analyst's reasoning:

${reasoning}

Here is the underlying data for reference:

${dataBlock}

Now structure this into the JSON format. Return JSON only. Do not add anything that is not genuinely in the reasoning above.`;

  return { system: systemString, user: userString };
}

// ─── D1: baseline_context ────────────────────────────────────────────────────
//
// Returns the single baseline_context row for (accountKey, weekEnding), or null
// if no row exists. Null is the "Step 5 has not produced a baseline for this
// account-week yet" signal — the caller decides whether that is a skip or a
// pipeline_issues log entry.
//
async function getBaselineContext(accountKey, weekEnding, env) {
  const row = await env.BASELINE_DB
    .prepare(
      `SELECT *
         FROM baseline_context
        WHERE account_key = ?
          AND week_ending = ?
        LIMIT 1`
    )
    .bind(accountKey, weekEnding)
    .first();
  return row || null;
}

// ─── D1: baseline_metrics ────────────────────────────────────────────────────
//
// Returns the flat array of baseline_metrics rows for (accountKey, weekEnding).
// Shape matches what formatBaselineBlock() iterates over:
//   [{ metric, window, stat, value }, ...]
// Up to 480 rows per account-week (20 metrics × 4 windows × 6 stats).
//
async function getBaselineMetrics(accountKey, weekEnding, env) {
  const res = await env.BASELINE_DB
    .prepare(
      `SELECT metric, window, stat, value
         FROM baseline_metrics
        WHERE account_key = ?
          AND week_ending = ?`
    )
    .bind(accountKey, weekEnding)
    .all();
  return res?.results || [];
}

// ─── D1: weekly_account_summary (last 12 weeks) ──────────────────────────────
//
// Returns up to 12 rows from weekly_account_summary, newest-first: this week
// plus the previous 11. formatHistoricalTrend() reverses internally so the
// prompt reads oldest-to-newest for natural streak detection.
//
async function getHistoricalWeeks(accountKey, weekEnding, env) {
  const res = await env.BASELINE_DB
    .prepare(
      `SELECT *
         FROM weekly_account_summary
        WHERE account_key = ?
          AND week_ending_date <= ?
        ORDER BY week_ending_date DESC
        LIMIT 12`
    )
    .bind(accountKey, weekEnding)
    .all();
  return res?.results || [];
}

// ─── Airtable: analysis_results (prior 2 weeks) ──────────────────────────────
//
// analysis_results lives in Airtable (not D1) and stores one row per issue per
// week. For the v2 prompt's continuity block we need the last 2 weeks'
// weekly_headline + top-priority issue.
//
// Strategy:
//   1. Filter where account_key matches AND week_ending_date is strictly
//      before the current weekEnding.
//   2. Sort by week_ending_date DESC, then priority_score DESC so the first
//      row per week is the highest-priority issue.
//   3. Group by week_ending_date, take the 2 most recent weeks.
//   4. Within each week the top row gives top_observation_{title,detail}.
//      weekly_headline sits on the first record of the group (written only
//      when written === 0 per v1 writeIssueRecord), so we prefer the row
//      where weekly_headline is populated but fall back to the top row.
//
// Returns [] if nothing available (cold-start weeks, new subscriber, etc.).
//
async function getPriorAnalyses(accountKey, weekEnding, env) {
  if (!accountKey || !weekEnding) return [];

  const formula = encodeURIComponent(
    `AND({account_key}='${accountKey}', IS_BEFORE({week_ending_date}, DATETIME_PARSE('${weekEnding}')))`
  );
  const sort =
    '&sort%5B0%5D%5Bfield%5D=week_ending_date&sort%5B0%5D%5Bdirection%5D=desc' +
    '&sort%5B1%5D%5Bfield%5D=priority_score&sort%5B1%5D%5Bdirection%5D=desc';

  // Cap at 100 records — two weeks of issues for a single account will never
  // exceed this in practice. No pagination loop needed for the continuity use.
  const url =
    `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_ANALYSIS_RESULTS}` +
    `?filterByFormula=${formula}${sort}&pageSize=100`;

  let res;
  try {
    res = await airtableGet(url, env);
  } catch (_) {
    // analysis_results read failures are non-fatal for prompt continuity —
    // the prompt handles empty priorAnalyses cleanly. Surface nothing here.
    return [];
  }
  const records = res?.records || [];

  // Group by week, preserving sort order (week DESC, priority DESC within).
  const byWeek = new Map();
  for (const r of records) {
    const wk = r?.fields?.week_ending_date;
    if (!wk) continue;
    if (!byWeek.has(wk)) byWeek.set(wk, []);
    byWeek.get(wk).push(r);
  }

  const weeks = Array.from(byWeek.keys()).slice(0, 2);

  return weeks.map(wk => {
    const rows        = byWeek.get(wk) || [];
    const top         = rows[0] || null;
    const headlineRow = rows.find(r => r?.fields?.weekly_headline) || top;
    return {
      week_ending:            wk,
      weekly_headline:        headlineRow?.fields?.weekly_headline || null,
      top_observation_title:  top?.fields?.issue_type               || null,
      top_observation_detail: top?.fields?.action_detail            || null,
    };
  });
}

// ─── CALL CLAUDE ──────────────────────────────────────────────────────────────

async function callClaude(systemPrompt, userMessage, env, maxTokens = 2500) {
  const res = await fetch(ANTHROPIC_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: maxTokens,
      system:     systemPrompt,
      messages:   [{ role: 'user', content: userMessage }],
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Claude API ${res.status}: ${text.slice(0, 200)}`);
  }

  const data = await res.json();
  return data.content?.[0]?.text || null;
}

// ─── PARSE AI RESPONSE ────────────────────────────────────────────────────────

function parseAIResponse(text) {
  try {
    const clean  = text.trim().replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();
    const parsed = JSON.parse(clean);
    const issues = Array.isArray(parsed.issues) ? parsed.issues.slice(0, 5) : [];
    const pattern_observations = Array.isArray(parsed.pattern_observations)
      ? parsed.pattern_observations
      : [];
    return {
      issues,
      summary:             parsed.summary             || null,
      weekly_headline:     parsed.weekly_headline     || null,
      pattern_observations,
      forward_preparation: parsed.forward_preparation || null,
    };
  } catch {
    return {
      issues: [],
      summary: null,
      weekly_headline: null,
      pattern_observations: [],
      forward_preparation: null,
    };
  }
}

// ─── BUILD CONTEXT SNAPSHOT ───────────────────────────────────────────────────

function buildContextSnapshot(weekly, baseline, account, profile) {
  return JSON.stringify({
    spend:           weekly?.spend,
    roas:            weekly?.account_roas,
    cpa:             weekly?.account_cpa,
    ctr:             weekly?.account_ctr,
    currency:        weekly?.currency || account?.account_currency,
    aov:             profile?.aov_typical || weekly?.account_aov,
    break_even_roas: profile?.break_even_roas,
    target_cpa:      profile?.target_cpa,
    baseline_roas:   baseline?.roas_4w_median,
    baseline_cpa:    baseline?.cpa_4w_median,
    roas_iqr:        baseline?.roas_4w_iqr,
    cpa_iqr:         baseline?.cpa_4w_iqr,
    trend:           baseline?.trend_direction,
    season_tags:     baseline?.season_tags,
    baseline_status: baseline?.baseline_status,
    weeks_history:   baseline?.weeks_of_history_available,
    wow_roas:        weekly?.wow_roas_change,
    wow_spend:       weekly?.wow_spend_change,
    click_to_atc:    weekly?.click_to_atc_rate,
    atc_to_ic:       weekly?.atc_to_ic_rate,
    ic_to_purchase:  weekly?.ic_to_purchase_rate,
  });
}

// ─── MAP BASELINE MODE ────────────────────────────────────────────────────────

function mapBaselineMode(mode) {
  return { 'None': 'None', '2w': '2w', '4w': '4w' }[mode] || 'None';
}

// ─── WRITE ISSUE RECORD ───────────────────────────────────────────────────────

async function writeIssueRecord(issue, meta, env) {
  const {
    accountKey, adAccountId, airtableRecId, weekEnding,
    issueKey, baselineMode, isColdStart,
    fullText, weeklyHeadline,
    patternObservations, forwardPreparation,
    contextSnapshot,
  } = meta;

  const fields = {
    fldqPAeUuCPtOhhoT: accountKey,
    fld4o1pOgeczOYoNN: adAccountId,
    fldfwUqiddubxMzsI: weekEnding,
    fldHY1sdWRMhPOSUF: weekEnding,
    fld69DiFJJhS9JXb8: issueKey,
    fldnTbwi1LLkfBdng: 'account',
    fldDTEJJBhOn9TObx: 'Performance',
    fldjFPJKauHMj6GgH: issue.issue_category     || null,
    fldh7FF4kZhzz97Ao: issue.issue_type         || null,
    fldO31821RDNkjtFC: issue.severity           || null,
    fldQAqIW2D7ISvNeJ: issue.confidence         || null,
    fldVO7LD5cYPHD4BG: issue.confidence_reason  || null,
    fldd35gzicjrsmqzw: issue.primary_metric_name     || null,
    fld2FmKvZ4Kt1E37X: issue.primary_metric_value    ?? null,
    fldzTXBFFaECZXgmb: issue.primary_metric_change_pct ?? null,
    fld8cNtUkYxMHjmRl: issue.secondary_metric_name   || null,
    fldKd32Mom8OezekT: issue.secondary_metric_value  ?? null,
    fldPPhBr2GVIqbDVL: issue.secondary_metric_change_pct ?? null,
    fld5sXa5JLMzfBIcf: issue.evidence_json ? JSON.stringify(issue.evidence_json) : null,
    fldbURBcMZbaBCjFq: issue.action_type        || null,
    fldC61YYaYu30ynCp: issue.action_detail      || null,
    fld6kP2Zf0dhIg2Uz: issue.owner              || null,
    fldbacDnJLPtQqsPP: issue.priority           || null,
    fldRp0ITgUBUrA95T: issue.priority_score != null ? issue.priority_score : null,
    fldb0Vao5oEcexOpT: issue.impact_type        || null,
    fldvRiI4q99evEThw: issue.impact_estimate_value ?? null,
    fld4u7G1K9zgeCC9y: issue.impact_estimate_unit  || null,
    fldwRxABIpgLZ4USv: issue.follow_up_date     || null,
    fldSZt14tZ7Un5xUY: issue.success_metric     || null,
    fldMbQPRBGprkX5Zn: issue.success_target     || null,
    fldIPAwhKsWFqhVRi: isColdStart,
    fldCzHwjntfBcNqH6: mapBaselineMode(baselineMode),
    fld8rAI5tytKsBnJg: fullText         || null,
    fldVVOZtaHLk1iDGy: contextSnapshot  || null,
    fldPZpAXJxxsCQQlM: weeklyHeadline   || null,   // weekly_headline
    fldhE5tV0ERnnvEij: patternObservations || null, // pattern_observations (JSON)
    fldDv4mcXquEjcPhi: forwardPreparation  || null, // forward_preparation
    ...(airtableRecId ? { fldxFfCg3tGaXE2Tj: [airtableRecId] } : {}),
  };

  const clean = Object.fromEntries(
    Object.entries(fields).filter(([_, v]) => v !== null && v !== undefined)
  );

  const url  = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_ANALYSIS_RESULTS}`;
  const body = {
    records:       [{ fields: clean }],
    performUpsert: { fieldsToMergeOn: ['issue_key'] },
    typecast:      true,
  };

  await airtablePatch(url, body, env);
}

// ─── UTILITIES ────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function withRetry(fn) {
  try {
    return await fn();
  } catch (e) {
    if (e.message.includes('429') || e.message.includes('503')) {
      await sleep(2000);
      return await fn();
    }
    throw e;
  }
}

async function airtableGet(url, env) {
  return withRetry(async () => {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Airtable GET ${res.status}: ${text.slice(0, 200)}`);
    }
    return res.json();
  });
}

async function airtablePatch(url, body, env) {
  return withRetry(async () => {
    const res = await fetch(url, {
      method: 'PATCH',
      headers: {
        Authorization: `Bearer ${env.AIRTABLE_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Airtable PATCH ${res.status}: ${text.slice(0, 200)}`);
    }
    return res.json();
  });
}
