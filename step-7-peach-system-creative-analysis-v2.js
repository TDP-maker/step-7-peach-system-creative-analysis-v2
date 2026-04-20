/**
 * ============================================================
 * STEP 7 v2 — PEACH SYSTEM CREATIVE AI ANALYSIS + BRIEFS
 * Worker name : step-7-peach-system-creative-analysis
 * Cron        : 30 4 * * 3  (Tuesday 8:30am GST / 4:30am UTC)
 * Version     : v2 (wiring in progress)
 *
 * Reads:
 *   Connected_Accounts      tblok1PRjLxqfjLnF
 *   creative_output_table   tblq6OICv2BlU0Nek  (this week's ads + 90-day history)
 *   baseline_current        baseline_current    (account baseline for context)
 *   client_profile          tbl7Q8iV2SZSSp4Wi  (objective, AOV, geo)
 *   D1: baseline_context    (currency / season / sufficiency — same D1 as Step 6 v2)
 *
 * Writes:
 *   ai_creative_analysis_results  tblfZwmJqj4lS01qo
 *   (plus two new fields for v2 — creative_pattern_observations,
 *    forward_preparation — see STEP-7-V2-DECISIONS.md pre-deploy checklist)
 *
 * Secrets: AIRTABLE_TOKEN, AIRTABLE_BASE_ID, ANTHROPIC_API_KEY
 * Bindings: BASELINE_DB (D1, same binding Step 6 v2 uses)
 *
 * Build-up order in this branch:
 *   B1. /health bump + new fetch helpers (getCreativeHistory,
 *       getBaselineContext)                                        [THIS COMMIT]
 *   B2. processAccount rewiring (parallel fetches, new prompt inputs,
 *       sentinel-split parsing, new Airtable field writes)         [next]
 *   C.  Decisions doc + pre-deploy checklist                       [next]
 *
 * Step 7 v2 prompt functions land in step-7-v2-prompts-DRAFT.js —
 * this worker carries its own inline copies once Phase B completes.
 * ============================================================
 *
 * BUGS FIXED FROM MAKE.COM VERSION:
 *  1. Module 1 filter broken — no field name → returned all accounts
 *     Fixed: proper weekly filter on week_ending_date
 *  2. Creative fetch had NO week filter — pulled top 10 ads ALL TIME
 *     Fixed: filters by week_ending_date AND ad_account_id
 *  3. Brief generation used choices[1] not choices[0] — always empty input
 *     Fixed: Claude API uses content[0].text correctly
 *  4. objective field never populated in text aggregator
 *     Fixed: objective read directly from creative_output_table
 *  5. v_week_ending_text never set → week_ending wrote blank
 *     Fixed: weekEnding passed correctly throughout
 *  6. account_name used wrong field name (account_name vs company_name)
 *     Fixed: reads company_name from Connected_Accounts
 *  7. No baseline context passed to AI
 *     Fixed: baseline_current fetched and passed to both prompts
 *  8. hook_text, image_tags, ad_format, video_duration, fatigue_flag ignored
 *     Fixed: all creative intelligence fields passed to AI
 *  9. Uses GPT-4.1
 *     Fixed: Claude (claude-sonnet-4-20250514)
 * 10. No upsert — created duplicate records on re-run
 *     Fixed: upsert by ad_account_id + week_ending_date
 *
 * CREATIVE_OUTPUT_TABLE FIELD IDs (read, tblq6OICv2BlU0Nek):
 *  fldB6BGdCUzvYelCv  ad_account_id
 *  fldZXQegutcmrd7j6  week_ending_date
 *  fldO2ktqAzMrPgbyI  week_ending
 *  fld8t4xi46kXNUdg0  ad_name
 *  fldwRTL916d0K1OzZ  ad_copy
 *  fldCsxk07zPP5bya2  headline
 *  fld61mCiNhsYAbchy  spend
 *  fldil23GWpVAhSmT1  purchases
 *  fldew5Zujx69Oqs6c  clicks
 *  fldKP6qLF3WO14jQW  impressions
 *  fldR2LCy9rfJauyzh  revenue
 *  fldPWmBv8YTzlvM6k  roas (formula)
 *  fld6AxrlzMuBEmZOo  leads
 *  fld4Ox7ShewTypTHe  cpl (formula)
 *  fldAS5dBZc2fN38O6  objective
 *  flda1SeswqZMwm4Fm  hook_text
 *  fldRwbf1YYZ1o164g  image_tags
 *  fldJ3wYhXkFxUKjS3  ad_format
 *  fldjPFA7sdKJgvZ1S  video_duration
 *  fldyMnKypSA0j7u68  fatigue_flag
 *  fldMBIa5eZjCK4DKR  thumbnail_url
 *  fldenFVWOAk0pEPge  instagram_permalink_url
 *  fldz20OZd7HiYx9sB  visual_text
 *  fldvzqsVxI3h75nRX  visual_hook_text
 *  fldM5UoYlL4jpaTS4  video_plays
 *  fldZsjg8xRJCqF1sm  video_plays_p25
 *  fldlBZsSE2y3jYsSU  video_plays_p50
 *  fldKfn7MvbycDQMQW  video_plays_p75
 *  fldzq7fNbL2AluKxG  video_plays_p100
 *  fldPVF4SKPLIHwmaY  video_thruplay
 *
 * AI_CREATIVE_ANALYSIS_RESULTS FIELD IDs (write, tblfZwmJqj4lS01qo):
 *  fldRZ6K7FuNhLcUlY  account_id
 *  fldfhYAuwV44nvE3f  account_name
 *  fldS88xA09OGcbWwM  ad_account_id
 *  fldL80gU8UZad9b0C  week_ending
 *  fldsNjvbEQOXdN54C  week_ending_date
 *  fld6deEwHeh4hNSod  creative_analysis
 *  fldDnf4XmsHoKdBLW  creative_brief
 * ============================================================
 */

const AIRTABLE_API           = 'https://api.airtable.com/v0';
const ANTHROPIC_API          = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL           = 'claude-sonnet-4-20250514'; // Update when newer model available

const TBL_CONNECTED_ACCOUNTS = 'tblok1PRjLxqfjLnF';
const TBL_CREATIVE_OUTPUT    = 'tblq6OICv2BlU0Nek';
const TBL_BASELINE_CURRENT   = 'baseline_current';
const TBL_CLIENT_PROFILE     = 'tbl7Q8iV2SZSSp4Wi';
const TBL_CREATIVE_RESULTS   = 'tblfZwmJqj4lS01qo';

const MIN_SPEND_FOR_ANALYSIS = 50; // Minimum spend per ad before including in analysis

// v2: creative-history fetch window + cap, and the sentinel the structuring
// call emits between the plain-text creative_analysis and the JSON tail
// carrying creative_pattern_observations + forward_preparation. See
// step-7-v2-prompts-DRAFT.js for the full prompt contract.
const HISTORY_WINDOW_DAYS         = 90;
const HISTORY_MAX_ADS_DEFAULT     = 50;
const PATTERNS_JSON_DELIMITER     = '===PATTERNS_JSON===';

// ─── ENTRY POINTS ────────────────────────────────────────────────────────────

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runCreativeAnalysis(env));
  },
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', step: 7, version: 'v2', worker: 'step-7-peach-system-creative-analysis' });
    }
    if (url.pathname === '/run') {
      ctx.waitUntil(runCreativeAnalysis(env));
      return Response.json({ status: 'started', message: 'Creative analysis running in background' });
    }
    if (url.pathname === '/run-single' && request.method === 'POST') {
      const accountId = url.searchParams.get('account_id');
      if (!accountId) return Response.json({ error: 'Missing account_id' }, { status: 400 });
      try {
        const result = await runSingleCreativeAccount(accountId, env);
        return Response.json({ success: true, result });
      } catch(e) {
        return Response.json({ success: false, error: e.message }, { status: 500 });
      }
    }
    return new Response('Not found', { status: 404 });
  }
};

// ─── SINGLE ACCOUNT HELPER ───────────────────────────────────────────────────

async function runSingleCreativeAccount(accountId, env) {
  const weekEnding = getLastSunday();
  const log = [`[SINGLE-7] account: ${accountId} week: ${weekEnding}`];
  const accounts = await getActiveAccounts(env);
  const account = accounts.find(a => {
    const id = a.fields.ad_account_id || '';
    const norm = id.startsWith('act_') ? id : `act_${id}`;
    const target = accountId.startsWith('act_') ? accountId : `act_${accountId}`;
    return norm === target || id === accountId;
  });
  if (!account) return { status: 'not_found', message: `No active account: ${accountId}` };
  const result = await processAccount(account, weekEnding, env, log);
  console.log(log.join('\n'));
  return { status: result, log };
}

// ─── MAIN ORCHESTRATOR ───────────────────────────────────────────────────────

async function runCreativeAnalysis(env) {
  const startTime = Date.now();
  const log = [`[STEP 7] Started at ${new Date().toISOString()}`];

  try {
    const weekEnding = getLastSunday();
    log.push(`Week ending: ${weekEnding}`);

    // Fetch active accounts
    const accounts = await getActiveAccounts(env);
    log.push(`Found ${accounts.length} active accounts`);

    if (accounts.length === 0) {
      log.push('No active accounts found');
      console.log(log.join('\n'));
      return;
    }

    let processed = 0, skipped = 0, errors = 0;

    for (const account of accounts) {
      try {
        const result = await processAccount(account, weekEnding, env, log);
        result === 'skipped' ? skipped++ : processed++;
      } catch (e) {
        errors++;
        log.push(`ERROR [${account.fields.ad_account_id}]: ${e.message}`);
      }
      await sleep(500);
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    log.push(`\n[STEP 7] Complete in ${elapsed}s — processed: ${processed}, skipped: ${skipped}, errors: ${errors}`);

  } catch (e) {
    log.push(`[STEP 7] FATAL: ${e.message}`);
  }

  console.log(log.join('\n'));
}

// ─── GET LAST SUNDAY ─────────────────────────────────────────────────────────

function getLastSunday() {
  const now = new Date();
  const day = now.getUTCDay(); // 0=Sun
  const sunday = new Date(now);
  sunday.setUTCDate(now.getUTCDate() - day);
  return sunday.toISOString().slice(0, 10);
}

// ─── FETCH ACTIVE ACCOUNTS ───────────────────────────────────────────────────

async function getActiveAccounts(env) {
  const formula   = encodeURIComponent(`OR({status}="active",{status}="Connected")`);
  const records   = [];
  let offset      = null;

  do {
    const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CONNECTED_ACCOUNTS}?filterByFormula=${formula}${offset ? `&offset=${offset}` : ''}`;
    const res = await airtableGet(url, env);
    records.push(...(res.records || []));
    offset = res.offset || null;
  } while (offset);

  return records;
}

// ─── PROCESS ONE ACCOUNT ─────────────────────────────────────────────────────

async function processAccount(account, weekEnding, env, log) {
  const f           = account.fields;
  const adAccountId = f.ad_account_id;
  const companyName = f.company_name || f.ad_account_name || adAccountId;
  const currency    = f['fldB6EBAu2iEJ8Qpm'] || f.account_currency || '';

  if (!adAccountId) {
    log.push(`SKIP [${account.id}]: no ad_account_id`);
    return 'skipped';
  }

  // v2: fetch this week's creatives, baseline, 90-day creative history, and
  // D1 baseline context (currency/season/sufficiency) in parallel. D1 is
  // optional — if the BASELINE_DB binding is missing, getBaselineContext
  // returns null and the prompts fall back to the Airtable baseline alone.
  const accountKey = f.account_key || adAccountId;
  const [creatives, baseline, creativeHistory, baselineContext] = await Promise.all([
    getWeeklyCreatives(adAccountId, weekEnding, env),
    getBaseline(adAccountId, f.account_key, env),
    getCreativeHistory(adAccountId, weekEnding, env, HISTORY_MAX_ADS_DEFAULT),
    getBaselineContext(accountKey, weekEnding, env),
  ]);

  await sleep(200);
  const clientProfile = await getClientProfile(account, env);

  if (creatives.length === 0) {
    log.push(`SKIP [${adAccountId}]: no creative data for week ${weekEnding}`);
    return 'skipped';
  }

  // Split into analysable (spend >= MIN) and insufficient data
  const analysable  = creatives.filter(c => (parseFloat(c.fields.spend) || 0) >= MIN_SPEND_FOR_ANALYSIS);
  const thinData    = creatives.filter(c => (parseFloat(c.fields.spend) || 0) < MIN_SPEND_FOR_ANALYSIS);

  // Skip if no creatives meet the spend threshold — nothing meaningful to analyse
  if (analysable.length === 0) {
    log.push(`SKIP [${adAccountId}]: all ${creatives.length} creative(s) below ${MIN_SPEND_FOR_ANALYSIS} ${currency} spend threshold`);
    return 'skipped';
  }

  log.push(`Analysing [${adAccountId}] — ${creatives.length} creatives (${analysable.length} analysable, ${thinData.length} insufficient spend); history: ${creativeHistory.length} ad(s) over ${HISTORY_WINDOW_DAYS}d; D1 ctx: ${baselineContext ? 'yes' : 'no'}`);

  // Determine objective — use most common from creatives, fall back to profile
  const objective = getMostCommonObjective(creatives) ||
                    clientProfile?.fields?.current_goal ||
                    'sales';

  const aov           = clientProfile?.fields?.aov_typical || null;
  const breakEvenROAS = clientProfile?.fields?.break_even_roas || null;
  const targetCPA     = clientProfile?.fields?.target_cpa || null;
  const primaryGeo    = clientProfile?.fields?.primary_geo || f.account_timezones || 'unknown';
  // brand_tone drives hook style and copy approach across all briefs
  // Options: playful | premium | professional | bold | warm | minimal
  // Stored in Connected_Accounts — add as a single select field there
  const brandTone     = f.brand_tone || f['brand_tone'] || 'warm'; // default: warm/accessible
  // Prefer D1 season_tags (authoritative) over Airtable baseline fall-back.
  const seasonTags    = baselineContext?.season_tags || baseline?.fields?.season_tags || 'standard';
  const seasonStatus  = getSeasonStatus(seasonTags, new Date().toISOString().split('T')[0]);

  // v2: pre-compute format aggregations from the 90-day creative history.
  // The prompt layer renders them; the worker does the arithmetic so the
  // model never has to tally.
  const formatAggregations = computeFormatAggregations(creativeHistory);

  // buildAnalysisPrompt returns a single data-block string in v2.
  const dataBlock = buildAnalysisPrompt({
    companyName, adAccountId, weekEnding, currency,
    objective, aov, breakEvenROAS, targetCPA, primaryGeo, seasonTags, seasonStatus,
    analysable, thinData,
    baseline: baseline?.fields,
    creativeHistory, formatAggregations, baselineContext,
  });

  const isLeadGen = /lead/i.test(objective);
  const isSales   = !isLeadGen && !/traffic|click/i.test(objective);

  // Call A: Free creative reasoning
  log.push(`[${adAccountId}] Step A: reasoning...`);
  const reasoningPrompt = buildCreativeReasoningPrompt(dataBlock);
  const reasoning = await callClaude(reasoningPrompt.system, reasoningPrompt.user, env, 1800);
  if (!reasoning) { log.push(`SKIP [${adAccountId}]: reasoning empty`); return 'skipped'; }
  log.push(`[${adAccountId}] Step B: structuring...`);
  await sleep(300);

  // Call B: Structure reasoning. Response carries plain-text analysis, then
  // PATTERNS_JSON_DELIMITER, then a JSON tail with creative_pattern_observations
  // and forward_preparation. Split below.
  const sufficiency = {
    '4w':           baselineContext?.sufficiency_4w           || 'insufficient',
    '12w':          baselineContext?.sufficiency_12w          || 'insufficient',
    'lifetime':     baselineContext?.sufficiency_lifetime     || 'insufficient',
    'seasonal_yoy': baselineContext?.sufficiency_seasonal_yoy || 'insufficient',
  };
  const structuringPrompt = buildCreativeStructuringPrompt(reasoning, dataBlock, {
    currency, isLeadGen, isSales, minSpend: MIN_SPEND_FOR_ANALYSIS,
    sufficiency,
    historyCount: creativeHistory.length,
    seasonTags, seasonStatus,
  });
  const structuringResponse = await callClaude(structuringPrompt.system, structuringPrompt.user, env, 2500);
  if (!structuringResponse) { log.push(`SKIP [${adAccountId}]: structuring empty`); return 'skipped'; }

  const { analysisText, creativePatternObservations, forwardPreparation, parseWarnings } =
    parseStructuringResponse(structuringResponse);
  for (const w of parseWarnings) log.push(`[${adAccountId}] ${w}`);

  log.push(`[${adAccountId}] Step C: briefs...`);
  await sleep(300);

  // Call C: Brief generation — now pattern-grounded via creativePatternObservations
  const { systemPrompt: briefSystem, userMessage: briefUser } = buildBriefPrompt({
    companyName, objective, currency, aov, seasonTags, seasonStatus, brandTone,
    analysisText,
    clientProfile: clientProfile?.fields,
    creativePatternObservations,
  });
  const briefResponse = await callClaude(briefSystem, briefUser, env, 3000);

  // Parse structured briefs from JSON response
  const briefs = briefResponse ? parseBriefResponse(briefResponse) : [];

  // Write to ai_creative_analysis_results — upsert by ad_account_id + week_ending_date
  await writeResults({
    adAccountId,
    companyName,
    weekEnding,
    analysisText,
    briefText:    briefResponse || 'Brief generation failed — analysis completed successfully.',
    briefs,
    creativePatternObservations,
    forwardPreparation,
  }, env);

  log.push(`DONE [${adAccountId}] — analysis + briefs written (patterns: ${creativePatternObservations.length}, forward_prep: ${forwardPreparation ? 'yes' : 'no'})`);
  return 'processed';
}

// ─── COMPUTE FORMAT AGGREGATIONS ─────────────────────────────────────────────
// Builds the row shape formatFormatAggregations expects:
//   { format, count, lifetime_spend, avg_roas }
// Sums spend and averages ROAS across the 90-day history, per ad_format.

function computeFormatAggregations(history) {
  if (!Array.isArray(history) || history.length === 0) return [];
  const byFormat = {};
  for (const rec of history) {
    const f = rec.fields || {};
    const format = f.ad_format || 'unknown';
    if (!byFormat[format]) {
      byFormat[format] = { format, count: 0, spendSum: 0, roasSum: 0, roasN: 0 };
    }
    byFormat[format].count += 1;
    byFormat[format].spendSum += parseFloat(f.spend) || 0;
    const roas = parseFloat(f.roas);
    if (!isNaN(roas)) {
      byFormat[format].roasSum += roas;
      byFormat[format].roasN += 1;
    }
  }
  return Object.values(byFormat).map(r => ({
    format: r.format,
    count: r.count,
    lifetime_spend: Math.round(r.spendSum),
    avg_roas: r.roasN ? Number((r.roasSum / r.roasN).toFixed(2)) : null,
  }));
}

// ─── PARSE STRUCTURING RESPONSE (v2) ─────────────────────────────────────────
// Splits the structuring call's output on PATTERNS_JSON_DELIMITER. Everything
// before the sentinel is the plain-text creative_analysis (unchanged v1
// contract). Everything after is parsed as JSON for the two new output fields.
// Degrades gracefully: if the sentinel is missing or the JSON is malformed,
// the full response is used as analysisText and the new fields default to
// empty array / null, preserving the frontend contract.

function parseStructuringResponse(text) {
  const warnings = [];
  const empty = {
    analysisText: text || '',
    creativePatternObservations: [],
    forwardPreparation: null,
    parseWarnings: warnings,
  };
  if (!text) return empty;

  const idx = text.indexOf(PATTERNS_JSON_DELIMITER);
  if (idx === -1) {
    warnings.push('structuring: sentinel missing — no pattern JSON parsed');
    return empty;
  }

  const analysisText = text.slice(0, idx).trimEnd();
  let jsonPart = text.slice(idx + PATTERNS_JSON_DELIMITER.length).trim();
  // Strip accidental markdown fences the model may add
  jsonPart = jsonPart.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();

  let parsed;
  try {
    parsed = JSON.parse(jsonPart);
  } catch (err) {
    warnings.push(`structuring: JSON parse failed (${err.message}) — falling back to empty patterns`);
    return { ...empty, analysisText };
  }

  const patterns = Array.isArray(parsed?.creative_pattern_observations)
    ? parsed.creative_pattern_observations.slice(0, 3).filter(p => p && typeof p === 'object')
    : [];
  const fwd = typeof parsed?.forward_preparation === 'string' && parsed.forward_preparation.trim()
    ? parsed.forward_preparation.trim()
    : null;

  return {
    analysisText,
    creativePatternObservations: patterns,
    forwardPreparation: fwd,
    parseWarnings: warnings,
  };
}

// ─── FETCH THIS WEEK'S CREATIVES ─────────────────────────────────────────────

async function getWeeklyCreatives(adAccountId, weekEnding, env) {
  // Normalise ID formats to search all variants
  const norm  = adAccountId.startsWith('act_') ? adAccountId : `act_${adAccountId}`;
  const plain = adAccountId.startsWith('act_') ? adAccountId.replace('act_', '') : adAccountId;

  // Filter by this week AND this account — sorted by ROAS desc, then spend desc
  const formula = encodeURIComponent(
    `AND(IS_SAME({week_ending_date},DATETIME_PARSE('${weekEnding}'),'day'),OR({ad_account_id}='${adAccountId}',{ad_account_id}='${norm}',{ad_account_id}='${plain}'))`
  );
  const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CREATIVE_OUTPUT}?filterByFormula=${formula}&sort%5B0%5D%5Bfield%5D=roas&sort%5B0%5D%5Bdirection%5D=desc&sort%5B1%5D%5Bfield%5D=spend&sort%5B1%5D%5Bdirection%5D=desc&maxRecords=20`;
  const res = await airtableGet(url, env);
  return res.records || [];
}

// ─── FETCH BASELINE ──────────────────────────────────────────────────────────

async function getBaseline(adAccountId, accountKey, env) {
  // Try account_key first, fall back to ad_account_id
  const key     = accountKey || adAccountId;
  const formula = encodeURIComponent(`{account_key}='${key}'`);
  const url     = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_BASELINE_CURRENT}?filterByFormula=${formula}&maxRecords=1`;
  const res     = await airtableGet(url, env);
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

// ─── FETCH 90-DAY CREATIVE HISTORY (v2) ──────────────────────────────────────
// Pulls up to `limit` ads from creative_output_table with week_ending_date
// inside the HISTORY_WINDOW_DAYS window, filtered to this ad account and to
// ads that cleared the MIN_SPEND_FOR_ANALYSIS threshold. Sorted by spend desc
// so the highest-signal ads survive the cap when accounts have > limit rows.
// Does NOT exclude this week — the history is the full 90-day picture, and
// the prompt layer separates "this week" vs. "prior weeks" for reasoning.

async function getCreativeHistory(adAccountId, weekEnding, env, limit = HISTORY_MAX_ADS_DEFAULT) {
  const norm  = adAccountId.startsWith('act_') ? adAccountId : `act_${adAccountId}`;
  const plain = adAccountId.startsWith('act_') ? adAccountId.replace('act_', '') : adAccountId;

  const formula = encodeURIComponent(
    `AND(` +
      `IS_AFTER({week_ending_date},DATEADD(DATETIME_PARSE('${weekEnding}'),-${HISTORY_WINDOW_DAYS},'days')),` +
      `OR({ad_account_id}='${adAccountId}',{ad_account_id}='${norm}',{ad_account_id}='${plain}'),` +
      `{spend}>=${MIN_SPEND_FOR_ANALYSIS}` +
    `)`
  );
  const url = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CREATIVE_OUTPUT}`
    + `?filterByFormula=${formula}`
    + `&sort%5B0%5D%5Bfield%5D=spend&sort%5B0%5D%5Bdirection%5D=desc`
    + `&maxRecords=${limit}`;

  try {
    const res = await airtableGet(url, env);
    return res.records || [];
  } catch (err) {
    console.error('getCreativeHistory failed:', err?.message || err);
    return [];
  }
}

// ─── FETCH BASELINE CONTEXT FROM D1 (v2) ─────────────────────────────────────
// Mirrors Step 6 v2 exactly. Requires the BASELINE_DB binding. Returns null
// if the binding is absent or the query fails, so the prompt layer degrades
// to the Airtable baseline_current record rather than throwing.

async function getBaselineContext(accountKey, weekEnding, env) {
  if (!env.BASELINE_DB) return null;
  try {
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
  } catch (err) {
    console.error('getBaselineContext failed:', err?.message || err);
    return null;
  }
}

// ─── GET MOST COMMON OBJECTIVE ───────────────────────────────────────────────

function getMostCommonObjective(creatives) {
  const counts = {};
  for (const c of creatives) {
    const obj = c.fields.objective;
    if (obj) counts[obj] = (counts[obj] || 0) + 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
}

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

// ─── formatCreativePatterns ──────────────────────────────────────────────────
//
// Renders the pattern observations (parsed from the structuring call's JSON
// tail) as a short prose block the brief generator can ground its
// why_it_works fields in. An empty array yields a clear "no patterns
// surfaced" note so the brief generator does not hallucinate patterns.
//
function formatCreativePatterns(patterns) {
  if (!Array.isArray(patterns) || patterns.length === 0) {
    return 'PATTERN OBSERVATIONS: none surfaced this week. Ground briefs in the analysis findings above rather than invented patterns.';
  }
  const lines = patterns.map((p, i) => {
    const title = (p && p.title)  ? String(p.title).trim()  : 'Untitled pattern';
    const type  = (p && p.pattern_type) ? String(p.pattern_type).trim() : 'unspecified';
    const body  = (p && p.detail) ? String(p.detail).trim() : '';
    return 'P' + (i + 1) + ' [' + type + '] ' + title + '\n  ' + body;
  });
  return 'PATTERN OBSERVATIONS (from this account\'s 90-day history):\n\n' + lines.join('\n\n');
}

// ─── buildBriefPrompt (CALL C) ───────────────────────────────────────────────
//
// Brief generation. v1's brand-tone system (eight tones, bit-exact) is
// preserved verbatim in the brandToneRules chain below — Design Brief §12.6
// locks this as the single strongest piece of craft in the worker.
//
// v2 additions vs v1:
//   - Signature: creativePatternObservations (array from the structuring
//     call's JSON tail) — rendered via formatCreativePatterns so the brief
//     generator can ground why_it_works in account-specific 90-day patterns
//     (Design Brief §4.7).
//   - System prompt: explicit "two voices in one brief" rule — creative
//     voice for hook / subheadline / visual_concept / copy_body / cta;
//     analyst voice for why_it_works and format_adaptations.
//   - JSON output: why_it_works shifts from v1's single-sentence assertion
//     ("proving this theme resonates") to a 2-4 sentence analyst-voice
//     pattern-grounded paragraph.
//
// The hook guidance (five hook styles with per-style examples) is preserved
// bit-exact from v1 — Design Brief §2.8 instructs us to extend hook
// variety, never to reduce it.
//
function buildBriefPrompt({
  companyName, objective, currency, aov, seasonTags, seasonStatus, brandTone,
  analysisText, clientProfile,
  creativePatternObservations,
}) {

  const isLeadGen = /lead/i.test(objective);

  const productType     = Array.isArray(clientProfile?.product_type)
                            ? clientProfile.product_type.join(', ')
                            : (clientProfile?.product_type || 'unknown');
  const targetAudience  = Array.isArray(clientProfile?.target_audience)
                            ? clientProfile.target_audience.join(', ')
                            : (clientProfile?.target_audience || 'unknown');
  const creativeFormats = Array.isArray(clientProfile?.creative_formats)
                            ? clientProfile.creative_formats.join(', ')
                            : (clientProfile?.creative_formats || 'all formats');
  const offerType       = Array.isArray(clientProfile?.offer_type)
                            ? clientProfile.offer_type.join(', ')
                            : (clientProfile?.offer_type || 'unknown');
  const purchaseSpeed   = clientProfile?.purchase_speed || 'unknown';

  const patternsBlock   = formatCreativePatterns(creativePatternObservations);

  // Brand tone system — bit-exact from v1. Design Brief §12.6 locks this
  // as the single strongest piece of craft in the worker. The if/else chain
  // mirrors v1's ternary exactly; each branch's body is copied verbatim
  // from v1 including the em dashes in the instructions (those appear in
  // the PROMPT to the model, not in the model's framing output, and so are
  // exempt from the analyst-voice "no em dashes" rule which applies to
  // framing text).
  //
  // Chunks fill this in stages: A5b-i covers premium/luxury, playful, bold;
  // A5b-ii covers professional, aspirational, cultural, urgent; A5b-iii
  // covers the warm/accessible default. Until the final chunk lands, any
  // unmatched tone falls through to the placeholder and is caught in
  // testing — this function is not wired into the worker until the whole
  // draft is complete.
  let brandToneRules;
  if (brandTone === 'premium' || brandTone === 'luxury' || brandTone === 'minimal') {
    brandToneRules = `PREMIUM/LUXURY BRAND. This brand competes on exclusivity, quality, and aspiration. The creative must reflect that.
HOOK RULES: No wordplay. No puns. No exclamation marks. No casual language. Hooks should be restrained, elegant, and aspirational. Fewer words carry more weight. Think: 'For moments that matter.' / 'Nothing ordinary. Ever.' / 'The art of celebration.'
COPY RULES: Short sentences. Sophisticated vocabulary. No discount language. No urgency tactics. The brand speaks with quiet confidence, not excitement.
NEVER: puns, exclamation marks, emoji, casual slang, price anchoring, countdown urgency.`;
  } else if (brandTone === 'playful' || brandTone === 'fun') {
    brandToneRules = `PLAYFUL BRAND. This brand's personality is fun, energetic, and joyful. Creative should reflect that energy.
HOOK RULES: Wordplay and puns are strongly encouraged — especially for seasonal moments. Keep them clever and relevant to the product. Celebration, delight, and smiles are the goals.
COPY RULES: Conversational, upbeat, warm. Short punchy sentences. Emojis acceptable if data shows they performed well. Brand voice is a friend, not a corporation.`;
  } else if (brandTone === 'bold') {
    brandToneRules = `BOLD BRAND. Direct, confident, no-nonsense. Makes strong statements.
HOOK RULES: Short, punchy, declarative. No hedging. 'This changes everything.' / 'You've been doing it wrong.' / 'Stop settling.'
COPY RULES: Short sentences. Active voice. Strong verbs. No fluff.`;
  } else if (brandTone === 'professional') {
    brandToneRules = `PROFESSIONAL BRAND. Credible, authoritative, trustworthy. Often B2B or considered-purchase categories.
HOOK RULES: Lead with credibility, results, or expertise. No puns. No casual language. 'The only solution your team needs.' / 'Trusted by 500 companies across the UAE.'
COPY RULES: Clear, factual, benefit-focused. Professional but not cold.`;
  } else if (brandTone === 'aspirational') {
    brandToneRules = `ASPIRATIONAL BRAND. Sits between warm and premium — lifestyle, mid-tier fashion, travel, home décor, wellness. The brand sells an elevated version of the audience's life.
HOOK RULES: Paint a picture of the life they want. Hooks should make people think 'I want to feel like that.' No puns. No hard sell. 'This is the life.' / 'You deserve this.' / 'The version of you that has this.' / 'Some things are worth it.'
COPY RULES: Evocative, sensory language. Paint scenes not features. Short sentences that breathe. Confident but not cold.`;
  } else if (brandTone === 'cultural') {
    brandToneRules = `CULTURAL BRAND. Community, heritage, tradition, and shared identity are central to this brand. Often relevant for Gulf market brands with deep cultural roots or brands targeting specific communities.
HOOK RULES: Speak to shared values, traditions, and moments of collective identity. Hooks should feel like they come from inside the community. 'For the family that celebrates together.' / 'Eid is more than a day. It's us.' / 'Rooted in tradition. Made for today.'
COPY RULES: Warm, inclusive, community-first language. Reference shared cultural moments authentically. Never appropriative — must be genuinely relevant to the brand's audience.`;
  } else if (brandTone === 'urgent' || brandTone === 'urgent/direct') {
    brandToneRules = `URGENT/DIRECT BRAND. Performance-first. Every word earns its place. Drives action immediately. Often used for ecommerce flash sales, limited offers, lead generation, or high-conversion campaigns.
HOOK RULES: Lead with the offer, the deadline, or the consequence of not acting. No fluff. 'Ends tonight.' / 'Only 12 left.' / 'Last chance to get this price.' / 'Your competitors are already using this.'
COPY RULES: Short. Direct. One idea per sentence. Strong CTA. Numbers where possible. Urgency must feel real — fabricated urgency destroys trust.`;
  } else {
    // Warm / accessible — the v1 default for any unmatched brand tone
    // value. Bit-exact from v1.
    brandToneRules = `WARM/ACCESSIBLE BRAND. Friendly, human, relatable. The majority of lifestyle and consumer brands.
HOOK RULES: Emotional connection, aspiration, identity, and occasion-based hooks work well. Subtle wordplay is acceptable when brand-relevant — not forced puns. Celebratory and joyful energy.
COPY RULES: Conversational but polished. Warm tone. Speaks to the person, not at them.`;
  }

  // Seasonal framing. v1 guidance preserved verbatim — this is voice and
  // market craft that should not be rewritten. Only the status-switching
  // wrapper is kept.
  const seasonalContextBlock = `## SEASONAL CONTEXT
Current season: ${seasonTags || 'standard'} (Status: ${seasonStatus || 'active'})
${seasonStatus === 'upcoming'
  ? `SEASON UPCOMING: ${seasonTags} is approaching. At least one brief should prepare seasonal creative ready to launch at season start.`
  : seasonStatus === 'active'
    ? (/ramadan|eid/i.test(seasonTags || '')
        ? `RAMADAN / EID WINDOW ACTIVE: This is one continuous campaign window — campaigns started in Ramadan carry through Eid al-Fitr. Write briefs that work across both periods. Urgency, generosity, celebration, and family themes perform strongly. At least one brief should leverage this seasonal context.`
        : `HIGH-COMPETITION SEASON ACTIVE: At least one brief should be season-specific with a seasonal hook and offer angle.`)
    : seasonStatus === 'recently_ended'
      ? `SEASON RECENTLY ENDED (within 2 weeks): Do NOT write new seasonal briefs — they cannot be produced and tested in time. Instead, identify what worked during the season (hook angles, offer types, urgency cues, creative formats) and adapt those principles to evergreen creative that will perform year-round. In why_it_works, explicitly name the seasonal learning you are adapting.`
      : ''}`;

  const systemPrompt = `You are a senior Meta ads creative strategist at a digital marketing agency. You translate performance analysis and the account's 90-day creative patterns into three distinct, immediately actionable creative briefs for new Meta ads. British English throughout.

## YOUR ROLE
Write briefs a designer and copywriter can execute without asking questions. Be specific, visual, and ground every direction in the performance analysis and the pattern observations below. Never invent angles not supported by the analysis or patterns.

## TWO VOICES IN ONE BRIEF — the rule
Each brief contains two voices.
- CREATIVE VOICE governs: hook, subheadline, visual_concept, copy_body, cta. Persuasive by design. Written for the audience, not the brand. Brand tone rules (below) apply here.
- ANALYST VOICE governs: why_it_works and format_adaptations. Observational, grounded in the analysis and the 90-day pattern data. Specific numbers, hedged causation. British English. No em dashes. No currency symbols. "Has consistently performed" not "proves X resonates".

If the text is going INTO a Meta ad, it is creative voice. If it is explaining the brief to the client, it is analyst voice.

## OBJECTIVE
${isLeadGen
  ? 'LEAD GENERATION — briefs should focus on capturing contact details. CTAs should drive form fills, messages, or calls.'
  : 'SALES / ECOMMERCE — briefs should drive purchase intent. CTAs should drive to product pages or checkout.'}

## CURRENCY
All numbers in ${currency}. Write figures as: 379 ${currency}. NEVER use £, $, €, ﷼ or any currency symbol. British English does not mean British pounds — the currency is ${currency}.

${seasonalContextBlock}

## CLIENT CONTEXT
- Product type: ${productType}
- Target audience: ${targetAudience}
- Available formats: ${creativeFormats}
- Offer type: ${offerType}
- Purchase speed: ${purchaseSpeed}
${aov ? `- AOV: ${aov} ${currency}` : ''}

## 90-DAY PATTERN OBSERVATIONS — ground briefs here, not just this-week's top ad
${patternsBlock}

Use these to ground why_it_works. When a pattern is relevant to a brief direction, reference it explicitly (e.g. "occasion-led carousels have consistently been your strongest format across the last 12 weeks, averaging 5.8 ROAS compared to 2.1 for feature-led creative"). If no pattern is relevant to a given brief, ground why_it_works in the specific finding from the analysis above. Do NOT invent patterns; if PATTERN OBSERVATIONS above says "none surfaced this week", ground briefs only in the analysis findings.

## BRAND TONE — THIS OVERRIDES ALL HOOK AND COPY DECISIONS
Brand tone: ${brandTone || 'warm'}

${brandToneRules}

## RULES
- Generate exactly 3 briefs
- Each brief must take a distinct creative direction — no two can share the same hook style or visual approach
- Base every direction on insights from the analysis above AND, where relevant, on the 90-day PATTERN OBSERVATIONS block above. Cite the specific insight or pattern that supports it.
- If the analysis flags something as underperforming, do NOT use that approach
- If a pattern observation shows a style or format consistently underperforms for this account, do NOT brief that direction
- Be designer-ready and copywriter-ready — specific enough to execute without a briefing call
- No asterisks, no markdown, no emojis unless analysis shows they performed well
- British English only

## OUTPUT — VALID JSON ONLY
No markdown fences, no preamble. Return exactly this structure:

{
  "briefs": [
    {
      "concept_name": "Short internal name for the creative team",
      "hook": "Opening line — max 10 words or 3 seconds of video. Study the brand, the top-performing ads, the product type, the season, and the audience carefully before writing. The hook must feel like it was written specifically for this brand and this moment — not a generic template. There are several hook styles available — choose the one that fits the brand energy and brief concept: (1) ASPIRATIONAL — creates desire or captures a feeling: 'She still talks about that birthday.' / 'This is what celebrations should feel like.' / 'Because some moments deserve more than ordinary.' (2) OCCASION/URGENCY — ties to a real moment in the audience's life: 'Eid is almost here.' / 'One week. One celebration. Make it count.' (3) WORDPLAY/PUNS — playful and memorable, especially powerful for celebration, lifestyle, food, and seasonal brands. A well-crafted pun stops the scroll through delight. Match the pun to the occasion and the product: for balloons on Mother's Day 'Surprise her with a pop of love.' For Easter party supplies 'Egg-citing party essentials are here.' For Eid 'Make it Eid-tastic.' For a birthday balloon bar 'Let's get this party pop-ping.' (4) IDENTITY — speaks to who they are: 'For the mum who makes every moment magical.' / 'You're the one who makes it special.' (5) SOCIAL PROOF/CURIOSITY — creates intrigue: 'This is Qatar's most gifted balloon arrangement.' The hook is about the AUDIENCE and their world — never about the business. The brand must not appear in the hook. Weak hooks lead with problems, stress, or negativity — these are almost always wrong for celebration brands. Ask: does this make someone smile, feel something, or think — that is exactly me?",
      "subheadline": "Supporting line max 15 words, or empty string if not needed",
      "visual_concept": "What the viewer sees — scene, style, motion, framing. Be specific. Base on image_tags and visual data from top-performing ads. Creative voice.",
      "copy_body": "Main ad text — max 125 characters. Creative voice.",
      "cta": "Button label and optional supporting caption. Creative voice.",
      "format_adaptations": "ANALYST VOICE. Feed (1080x1080): how the visual reframes in square. Stories/Reels (1080x1920): how it reframes for vertical. Observational and specific, no em dashes, British English.",
      "why_it_works": "ANALYST VOICE. Two to four sentences grounding this brief in the analysis and, where relevant, in the 90-day pattern observations. Reference account-specific patterns by name where applicable (e.g. 'Occasion-led carousels have consistently been your strongest format across the last 12 weeks, averaging 5.8 ROAS compared to 2.1 for feature-led creative. This brief tests the same pattern with a sharper gifting focus.'). When no relevant pattern exists, reference a specific finding from the analysis — name the actual ad or metric. Observational not assertive. Hypothesis-plural when causation is uncertain. 'Has consistently performed' not 'proves X resonates'. British English. No em dashes. No currency symbols."
    },
    { "same structure for brief 2" },
    { "same structure for brief 3" }
  ]
}

RULES:
- Exactly 3 objects in the briefs array — no more, no less
- why_it_works and format_adaptations are ANALYST VOICE. Observational, grounded in the analysis or pattern observations. No 'proves X resonates' language. No em dashes. No currency symbols. British English.
- why_it_works must reference either a specific finding from the analysis (naming actual ad names or metrics) OR a specific entry from the PATTERN OBSERVATIONS block above. No generic statements.
- If PATTERN OBSERVATIONS says 'none surfaced this week', why_it_works grounds only in the analysis — do NOT fabricate a pattern.
- visual_concept must be grounded in image_tags or visual elements from the analysis — do not invent visual styles not evidenced in the data
- copy_body must be 125 characters or fewer
- hook must be 10 words or fewer
- hook must be written specifically for this brand, audience, product, and season — never a generic template
- hook must NEVER mention the brand name or business
- choose the right hook style for the brief: aspirational, occasion/urgency, wordplay/pun, identity, or curiosity
- for celebration, lifestyle, seasonal, and gifting brands — consider wordplay and puns seriously. A clever pun stops the scroll through delight and is more memorable than a generic statement
- seasonal puns work especially well: match the wordplay to the occasion and the product category
- hooks that lead with negativity, stress, or problems are almost always wrong for celebration brands
- urgency hooks must come from the audience's life — an occasion, a date — not from stock or sales pressure
- ask: does this make someone smile, feel something, or think 'that is exactly me'?
- if the hook could run for any brand in any category, it is not specific enough — rewrite it
- No currency symbols — numbers only
- British English throughout`;

  const userMessage = 'Based on this creative performance analysis for ' + companyName + ', generate three creative briefs:\n\n' + analysisText;

  return { systemPrompt, userMessage };
}

// ─── EXPORTS ─────────────────────────────────────────────────────────────────
// Subsequent chunks (A5b, A5c) replace the placeholders in buildBriefPrompt
// with the v1 brand tone system (bit-exact) and the analyst-voice +
// ─── CALL CLAUDE ──────────────────────────────────────────────────────────────

async function callClaude(systemPrompt, userMessage, env, maxTokens = 3000) {
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

// ─── PARSE BRIEF RESPONSE ────────────────────────────────────────────────────

function parseBriefResponse(text) {
  if (!text) return []; // Claude returned null — no briefs to parse
  try {
    const clean  = text.trim().replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();
    const parsed = JSON.parse(clean);
    const briefs = Array.isArray(parsed.briefs) ? parsed.briefs.slice(0, 3) : [];
    return briefs;
  } catch {
    return []; // parse failure — falls back to text blob only
  }
}



async function writeResults({ adAccountId, companyName, weekEnding,
                               analysisText, briefText, briefs,
                               creativePatternObservations, forwardPreparation }, env) {

  // Build structured brief fields from parsed briefs array
  const briefFields = {};
  const briefMap = [
    [0, {
      concept_name:       'fldO0u7lgNh1uwfSg', // brief_1_concept_name
      hook:               'fldY5TyHRBLYV4hO2', // brief_1_hook
      subheadline:        'fldCvikYWGScfWlJq', // brief_1_subheadline
      visual_concept:     'flduD9BwXTfAIAANN', // brief_1_visual_concept
      copy_body:          'fldDwc5ou3EJIKRm7', // brief_1_copy_body
      cta:                'fldvHSBUb2pfFIMdf', // brief_1_cta
      format_adaptations: 'fldmnBxLwLBMm28gZ', // brief_1_format_adaptations
      why_it_works:       'fldcxSViOgJ8pnMRZ', // brief_1_why_it_works
    }],
    [1, {
      concept_name:       'fldi23Ady2G3JUswc', // brief_2_concept_name
      hook:               'fldzQb87ymDv8MXNB', // brief_2_hook
      subheadline:        'fldtSTRSeFcG1Rx8J', // brief_2_subheadline
      visual_concept:     'fldFwwDtFMgBfOAUq', // brief_2_visual_concept
      copy_body:          'fldmD3itwtdswapGZ', // brief_2_copy_body
      cta:                'fld2YYbQd21JcSLkz', // brief_2_cta
      format_adaptations: 'fld4sOVh20JcEJVNP', // brief_2_format_adaptations
      why_it_works:       'fldim6BwQSYv6UfoE', // brief_2_why_it_works
    }],
    [2, {
      concept_name:       'fldoiJdNjagpfvVbH', // brief_3_concept_name
      hook:               'fldZNg6OvMqKSy6S8', // brief_3_hook
      subheadline:        'flduxpS5iRmIm8WmP', // brief_3_subheadline
      visual_concept:     'fld3GGdIj1VZgNPRY', // brief_3_visual_concept
      copy_body:          'fld86KPPhWssmNIXu', // brief_3_copy_body
      cta:                'flde5miuDyVkG7967', // brief_3_cta
      format_adaptations: 'fld8yvzXHE1NqgMWM', // brief_3_format_adaptations
      why_it_works:       'fldPQbTofz6aXIA69', // brief_3_why_it_works
    }],
  ];

  // Write each brief's fields — only if value exists and is non-empty
  for (const [idx, fieldIds] of briefMap) {
    const brief = briefs?.[idx];
    if (!brief) continue;
    for (const [key, fieldId] of Object.entries(fieldIds)) {
      const val = brief[key];
      if (val && String(val).trim()) {
        briefFields[fieldId] = String(val).trim();
      }
    }
  }

  const fields = {
    fldRZ6K7FuNhLcUlY: adAccountId,    // account_id
    fldS88xA09OGcbWwM: adAccountId,    // ad_account_id
    fldfhYAuwV44nvE3f: companyName,    // account_name
    fldL80gU8UZad9b0C: weekEnding,     // week_ending
    fldsNjvbEQOXdN54C: weekEnding,     // week_ending_date
    fld6deEwHeh4hNSod: analysisText,   // creative_analysis
    fldDnf4XmsHoKdBLW: briefText,      // creative_brief (full text fallback)
    ...briefFields,                    // 24 structured brief fields
  };

  // v2 new fields — written by NAME (not field ID) so the worker can ship
  // before Peach finalises field IDs. Requires typecast:true (already set)
  // and the two fields to exist in ai_creative_analysis_results. Once field
  // IDs are available, replace the keys below. See STEP-7-V2-DECISIONS.md.
  if (Array.isArray(creativePatternObservations) && creativePatternObservations.length > 0) {
    fields.creative_pattern_observations = JSON.stringify(creativePatternObservations);
  }
  if (forwardPreparation && String(forwardPreparation).trim()) {
    fields.forward_preparation = String(forwardPreparation).trim();
  }

  const url  = `${AIRTABLE_API}/${env.AIRTABLE_BASE_ID}/${TBL_CREATIVE_RESULTS}`;
  const body = {
    records:       [{ fields }],
    performUpsert: { fieldsToMergeOn: ['ad_account_id', 'week_ending_date'] },
    typecast:      true,
  };

  await airtablePatch(url, body, env);
}

// ─── UTILITIES ────────────────────────────────────────────────────────────────

// ─── ISLAMIC CALENDAR — ANCHOR-BASED ALGORITHM ───────────────────────────────
// Calculates Ramadan/Eid dates algorithmically — no manual updates needed.
// Accuracy: ±1-3 days. Sufficient for creative strategy purposes.
//
// Anchor: Ramadan 2026 started 2026-02-18 (verified against actual dates).
// Hijri year = 354.367 days average. Ramadan shifts ~10.9 days earlier per year.
// Eid al-Fitr = 30 days after Ramadan start (1 Shawwal).
// Eid al-Adha = ~70 days after Eid al-Fitr (10 Dhul Hijja).
//
// Handles edge case: two Ramadans in one Gregorian year (e.g. 2030).

const RAMADAN_ANCHOR_YEAR = 2026;
const RAMADAN_ANCHOR_MS   = new Date('2026-02-18').getTime();
const HIJRI_YEAR_MS       = 354.367 * 24 * 60 * 60 * 1000;

function addDaysMs(ms, n) { return ms + n * 86400000; }
function isoFromMs(ms) { return new Date(ms).toISOString().split('T')[0]; }

function getIslamicDates(gregorianYear) {
  const results = [];
  // Check surrounding years to catch edge cases
  for (const y of [gregorianYear - 1, gregorianYear, gregorianYear + 1]) {
    const yearDiff   = y - RAMADAN_ANCHOR_YEAR;
    const ramStartMs = RAMADAN_ANCHOR_MS + Math.round(yearDiff * HIJRI_YEAR_MS);
    const eidFitrMs  = addDaysMs(ramStartMs, 30);   // 1 Shawwal
    const eidAdhaMs  = addDaysMs(eidFitrMs,  70);   // 10 Dhul Hijja ~70 days later

    const ramYear = new Date(ramStartMs).getFullYear();
    const eidYear = new Date(eidFitrMs).getFullYear();

    if (ramYear === gregorianYear || eidYear === gregorianYear) {
      results.push({
        ramStart:    isoFromMs(ramStartMs),
        ramEnd:      isoFromMs(addDaysMs(ramStartMs, 29)),
        eidFitrStart: isoFromMs(eidFitrMs),
        eidFitrEnd:   isoFromMs(addDaysMs(eidFitrMs, 3)),
        eidAdhaStart: isoFromMs(eidAdhaMs),
        eidAdhaEnd:   isoFromMs(addDaysMs(eidAdhaMs, 3)),
      });
    }
  }
  // Deduplicate by ramStart
  const seen = new Set();
  return results.filter(r => { if (seen.has(r.ramStart)) return false; seen.add(r.ramStart); return true; });
}

// ─── SEASON STATUS ───────────────────────────────────────────────────────────
// Returns 'upcoming', 'active', 'recently_ended' (within 14 days), or 'past'
//
// KEY PRINCIPLE: Ramadan and Eid al-Fitr are ONE continuous campaign window.
function getSeasonStatus(seasonTags, today) {
  if (!seasonTags || seasonTags === 'standard') return 'standard';

  const tag       = seasonTags.toLowerCase();
  const todayDate = new Date(today);
  const year      = todayDate.getFullYear();
  const todayMs   = todayDate.getTime();

  // ── ISLAMIC SEASONS (algorithmically calculated) ──────────────────────────
  const islamic = getIslamicDates(year);

  for (const d of islamic) {
    // Ramadan + Eid al-Fitr = ONE campaign window (start of Ramadan to end of Eid)
    const ramEidWindowStart = new Date(d.ramStart).getTime();
    const ramEidWindowEnd   = new Date(d.eidFitrEnd).getTime();

    if (tag.includes('ramadan') || tag.includes('eid_al_fitr') || tag.includes('eid al-fitr') ||
        (tag.includes('eid') && !tag.includes('adha'))) {
      const daysSinceEnd = (todayMs - ramEidWindowEnd) / 86400000;
      if (todayMs < ramEidWindowStart) return 'upcoming';
      if (daysSinceEnd < 0)  return 'active';
      if (daysSinceEnd <= 14) return 'recently_ended';
      return 'past';
    }

    // Eid al-Adha
    if (tag.includes('eid_al_adha') || tag.includes('eid al-adha') || tag.includes('adha')) {
      const adhaStart = new Date(d.eidAdhaStart).getTime();
      const adhaEnd   = new Date(d.eidAdhaEnd).getTime();
      const daysSinceEnd = (todayMs - adhaEnd) / 86400000;
      if (todayMs < adhaStart) return 'upcoming';
      if (daysSinceEnd < 0)  return 'active';
      if (daysSinceEnd <= 14) return 'recently_ended';
      return 'past';
    }
  }

  // ── ALGORITHMICALLY CALCULATED SEASONS ──────────────────────────────────

  // Easter (Gregorian)
  function easter(y) {
    const a=y%19,b=Math.floor(y/100),c=y%100,d=Math.floor(b/4),e=b%4,
          f=Math.floor((b+8)/25),g=Math.floor((b-f+1)/3),
          h=(19*a+b-d-g+15)%30,i=Math.floor(c/4),k=c%4,
          l=(32+2*e+2*i-h-k)%7,m=Math.floor((a+11*h+22*l)/451),
          month=Math.floor((h+l-7*m+114)/31),day=((h+l-7*m+114)%31)+1;
    return new Date(y, month-1, day).getTime();
  }
  function nthWeekday(y, month0, weekday, n) {
    // nth occurrence of weekday (0=Sun) in month
    const d = new Date(y, month0, 1);
    const first = (weekday - d.getDay() + 7) % 7;
    return new Date(y, month0, 1 + first + (n-1)*7).getTime();
  }

  // Diwali anchor: 2024-11-01, Hindu year ~354.367 days
  const diwaliMs = new Date('2024-11-01').getTime() + Math.round((year-2024)*354.367*86400000);
  // Holi anchor: 2024-03-25
  const holiMs   = new Date('2024-03-25').getTime() + Math.round((year-2024)*354.367*86400000);

  const easterMs    = easter(year);
  const thanksMs    = nthWeekday(year, 10, 4, 4); // 4th Thursday November
  const blackFriMs  = thanksMs + 86400000;
  const whiteFriMs  = thanksMs + 86400000; // same window as Black Friday in UAE

  const algoSeasons = [
    { keys: ['easter'],                     start: easterMs - 2*86400000, end: easterMs + 86400000 },
    { keys: ['mothers_day_uk', "mother's day uk", 'mothering sunday'], start: easterMs - 21*86400000, end: easterMs - 21*86400000 + 86400000 },
    { keys: ['mothers_day_usa', "mother's day usa", "mother's day"], start: nthWeekday(year,4,0,2), end: nthWeekday(year,4,0,2)+86400000 },
    { keys: ['mothers_day'],                start: easterMs - 21*86400000, end: nthWeekday(year,4,0,2)+86400000 }, // covers both
    { keys: ['fathers_day', "father's day"], start: nthWeekday(year,5,0,3), end: nthWeekday(year,5,0,3)+86400000 },
    { keys: ['thanksgiving'],               start: thanksMs, end: thanksMs+86400000 },
    { keys: ['black_friday', 'black friday', 'bfcm', 'cyber monday'], start: blackFriMs, end: blackFriMs+4*86400000 },
    { keys: ['white_friday', 'white friday'], start: whiteFriMs-86400000, end: whiteFriMs+3*86400000 },
    { keys: ['diwali'],                     start: diwaliMs, end: diwaliMs+2*86400000 },
    { keys: ['holi'],                       start: holiMs, end: holiMs+86400000 },
  ];

  for (const s of algoSeasons) {
    if (s.keys.some(k => tag.includes(k))) {
      const daysSinceEnd = (todayMs - s.end) / 86400000;
      if (todayMs < s.start) return 'upcoming';
      if (daysSinceEnd < 0)  return 'active';
      if (daysSinceEnd <= 14) return 'recently_ended';
      return 'past';
    }
  }

  // ── FIXED-DATE SEASONS ────────────────────────────────────────────────────
  const fixedSeasons = [
    { keys: ['valentines', "valentine's", 'valentine'],    start: `${year}-02-07`,   end: `${year}-02-14` },
    { keys: ['christmas', 'xmas'],                         start: `${year}-12-24`,   end: `${year}-12-26` },
    { keys: ['new_year', 'new year'],                      start: `${year}-12-28`,   end: `${year+1}-01-02` },
    { keys: ['dsf', 'dubai shopping festival'],            start: `${year}-12-15`,   end: `${year+1}-01-26` },
    { keys: ['uae_national', 'uae national day'],          start: `${year}-12-02`,   end: `${year}-12-03` },
    { keys: ['qatar_national', 'qatar national day'],      start: `${year}-12-18`,   end: `${year}-12-18` },
    { keys: ['f1_abu_dhabi', 'abu dhabi grand prix', 'f1'], start: `${year}-11-26`,  end: `${year}-11-29` },
    { keys: ['back_to_school', 'back to school'],          start: `${year}-08-15`,   end: `${year}-09-15` },
    { keys: ['summer'],                                    start: `${year}-06-01`,   end: `${year}-08-31` },
  ];

  for (const season of fixedSeasons) {
    if (season.keys.some(k => tag.includes(k))) {
      const startMs = new Date(season.start).getTime();
      const endMs   = new Date(season.end).getTime();
      const daysSinceEnd = (todayMs - endMs) / 86400000;
      if (todayMs < startMs) return 'upcoming';
      if (daysSinceEnd < 0)  return 'active';
      if (daysSinceEnd <= 14) return 'recently_ended';
      return 'past';
    }
  }

  return 'active'; // unknown season tag — assume active
}

function sanitise(str) {
  if (!str) return '';
  return String(str).replace(/[\uD800-\uDFFF]/g, '');
}

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

// ─── AIRTABLE HTTP HELPERS ────────────────────────────────────────────────────

async function airtableGet(url, env) {
  return withRetry(async () => {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` }
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
