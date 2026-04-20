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

  // Fetch this week's creatives, baseline and client profile in parallel
  const [creatives, baseline] = await Promise.all([
    getWeeklyCreatives(adAccountId, weekEnding, env),
    getBaseline(adAccountId, f.account_key, env),
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

  log.push(`Analysing [${adAccountId}] — ${creatives.length} creatives (${analysable.length} analysable, ${thinData.length} insufficient spend)`);

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
  const seasonTags    = baseline?.fields?.season_tags || 'standard';
  const seasonStatus  = getSeasonStatus(seasonTags, new Date().toISOString().split('T')[0]);

  const { userMessage: dataBlock } = buildAnalysisPrompt({
    companyName, adAccountId, weekEnding, currency,
    objective, aov, breakEvenROAS, targetCPA, primaryGeo, seasonTags,
    analysable, thinData, baseline: baseline?.fields,
  });

  const isLeadGen = /lead/i.test(objective);
  const isSales   = !isLeadGen && !/traffic|click/i.test(objective);

  // Call A: Free creative reasoning
  log.push(`[${adAccountId}] Step A: reasoning...`);
  const reasoningPrompt = buildCreativeReasoningPrompt(dataBlock);
  const reasoning = await callClaude(reasoningPrompt.system, reasoningPrompt.user, env, 1500);
  if (!reasoning) { log.push(`SKIP [${adAccountId}]: reasoning empty`); return 'skipped'; }
  log.push(`[${adAccountId}] Step B: structuring...`);
  await sleep(300);

  // Call B: Structure reasoning
  const structuringPrompt = buildCreativeStructuringPrompt(reasoning, dataBlock, {
    currency, isLeadGen, isSales, minSpend: MIN_SPEND_FOR_ANALYSIS,
  });
  const analysisResponse = await callClaude(structuringPrompt.system, structuringPrompt.user, env, 2000);
  if (!analysisResponse) { log.push(`SKIP [${adAccountId}]: structuring empty`); return 'skipped'; }
  log.push(`[${adAccountId}] Step C: briefs...`);
  await sleep(300);

  // Call C: Brief generation
  const { systemPrompt: briefSystem, userMessage: briefUser } = buildBriefPrompt({
    companyName, objective, currency, aov, seasonTags, seasonStatus, brandTone,
    analysisText: analysisResponse,
    clientProfile: clientProfile?.fields,
  });
  const briefResponse = await callClaude(briefSystem, briefUser, env, 3000);

  // Parse structured briefs from JSON response
  const briefs = briefResponse ? parseBriefResponse(briefResponse) : [];

  // Write to ai_creative_analysis_results — upsert by ad_account_id + week_ending_date
  await writeResults({
    adAccountId,
    companyName,
    weekEnding,
    analysisText: analysisResponse,
    briefText:    briefResponse || 'Brief generation failed — analysis completed successfully.',
    briefs,
  }, env);

  log.push(`DONE [${adAccountId}] — analysis + briefs written`);
  return 'processed';
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

// ─── FORMAT CREATIVE FOR PROMPT ──────────────────────────────────────────────

function formatCreative(c, currency, index) {
  const f        = c.fields;
  const spend    = parseFloat(f.spend) || 0;
  const roas     = parseFloat(f.roas) || 0;
  const cpa      = parseFloat(f.cpl) || 0;
  const isVideo  = ['reel', 'video', 'story'].includes(f.ad_format);

  const lines = [
    `AD ${index + 1}: ${f.ad_name || 'Unnamed'}`,
    `Format: ${f.ad_format || 'unknown'} | Spend: ${spend} ${currency} | ROAS: ${roas || 'n/a'} | Purchases: ${f.purchases ?? 'n/a'} | Leads: ${f.leads ?? 'n/a'} | CPL: ${cpa || 'n/a'}`,
  ];

  if (f.hook_text)        lines.push(`Hook: ${sanitise(f.hook_text).slice(0, 150)}`);
  if (f.ad_copy)          lines.push(`Copy: ${sanitise(f.ad_copy).slice(0, 200)}${sanitise(f.ad_copy).length > 200 ? '...' : ''}`);
  if (f.headline)         lines.push(`Headline: ${sanitise(f.headline).slice(0, 150)}`);
  if (f.visual_text)      lines.push(`Visual text: ${sanitise(f.visual_text).slice(0, 150)}`);
  if (f.image_tags)       lines.push(`Visual tags: ${sanitise(f.image_tags).slice(0, 200)}`);
  if (f.fatigue_flag)     lines.push(`⚠️ FATIGUE FLAG: This ad is showing signs of creative fatigue`);

  if (isVideo) {
    if (f.video_duration) lines.push(`Video duration: ${f.video_duration}s`);
    if (f.video_plays_p25 != null) {
      lines.push(`Retention: 25%=${f.video_plays_p25} | 50%=${f.video_plays_p50 ?? 'n/a'} | 75%=${f.video_plays_p75 ?? 'n/a'} | 100%=${f.video_plays_p100 ?? 'n/a'} | Thruplay=${f.video_thruplay ?? 'n/a'}`);
    }
  }

  return lines.join('\n');
}

// ─── CHAIN-OF-THOUGHT STEP A: FREE CREATIVE REASONING ────────────────────────

function buildCreativeReasoningPrompt(dataBlock) {
  return {
    system: `You are a senior Meta ads creative strategist at The Digital Peach, a performance marketing agency. You work with businesses across multiple markets globally.

Before forming any view, read the account data carefully: the geography field tells you which market this account operates in. Your analysis must be grounded in that specific market.

Once you understand the market, apply relevant knowledge:
- Gulf region (UAE, Saudi Arabia, Qatar): social proof and urgency culture, WhatsApp-first communication, Arabic/English bilingual audiences, Ramadan/Eid/White Friday seasonality
- Western markets (UK, US, EU): different trust signals, longer consideration cycles
- Global accounts: consider which markets are driving performance and what that implies creatively
- If geography is unclear, note it honestly and analyse based on what the data shows

Think through what is actually happening with this account's creative performance this week. What patterns do you see? What is genuinely working and why? What is failing and what is the real reason? What should the team focus on?

Do not produce a formatted report yet. Write 3-5 paragraphs of honest, specific thinking. Reference actual ad names and actual numbers. If data is thin, say so. If something is counterintuitive, call it out.

British English throughout.

CRITICAL — CURRENCY: The account currency is stated in the data. Use ONLY that currency code when referencing spend, ROAS, CPA, CPL figures. NEVER use £, $, €, or any currency symbol. Write numbers only, followed by the currency code. Example: 379 QAR, not £379 or $379.`,
    user: dataBlock + '\n\nFirst identify the market from the geography data. Then think through these creatives. What is actually going on?'
  };
}

// ─── CHAIN-OF-THOUGHT STEP B: STRUCTURE CREATIVE REASONING ───────────────────

function buildCreativeStructuringPrompt(reasoning, dataBlock, params) {
  const { currency, isLeadGen, isSales, minSpend } = params;
  const metric = isSales ? 'ROAS: X | Purchases: X' : isLeadGen ? 'Leads: X | CPL: X' : 'Clicks: X | CPC: X';

  return {
    system: `You are structuring a senior creative strategist's analysis into a specific format. The analysis has already been done — extract the key findings and present them clearly. Do not add new conclusions. Do not pad. Preserve the honesty and specificity of the original thinking. British English throughout.

## FEW-SHOT EXAMPLES of excellent entries:

SCALE THESE example:
Party Shop destination video
Spend: 420 | ROAS: 11.4 | Purchases: 9
Why: Hook retention at 38% means nearly 4 in 10 viewers watch past the opening. The product demonstration angle is connecting directly with purchase-intent audience. Clear account winner.

CONSIDER PAUSING example:
Laduree still
Spend: 130 | ROAS: 1.2 | Purchases: 1
Why: Past the reliable data threshold with only 1 purchase. At 1.2 ROAS this creative is destroying value — reallocate budget to the winning video.

WATCH CLOSELY example:
New UGC clip
Spend: 85 | ROAS: 3.8 | Purchases: 2
Why: Early signals are promising at this spend level but 2 purchases is not enough to be confident. Give it another week.

## OUTPUT — plain text, no markdown, exactly this structure:

SCALE THESE

[Ad name]
Spend: X | ${metric}
Why: [One specific sentence citing the data point]

[Up to 3 ads]


CONSIDER PAUSING

[Ad name]
Spend: X | ${metric}
Why: [One specific sentence on why — reference the number]

[Up to 3 ads]


WATCH CLOSELY

[Ad name]
Spend: X | ${metric}
Why: [Promising but needs more data — be specific]

[Up to 3 ads — omit entirely if none]


INSUFFICIENT DATA

[Ads below ${minSpend} ${currency} spend — one line each]


KEY INSIGHT

[One sentence: the single most important pattern separating winners from losers this week]


RECOMMENDED ACTION

[One specific, immediately actionable next step — name the actual ad and the actual action]

Rules: Extract only what is in the reasoning. Name actual ads. Every Why references a specific number. British English. NEVER use £, $, or € — numbers only followed by the account currency code.`,
    user: `Here is the strategist's thinking:\n\n${reasoning}\n\nData for reference:\n\n${dataBlock}\n\nStructure this into the format. Extract the findings — do not add new ones.`
  };
}

// ─── BUILD CREATIVE ANALYSIS PROMPT ─────────────────────────────────────────

function buildAnalysisPrompt({ companyName, adAccountId, weekEnding, currency,
                                objective, aov, breakEvenROAS, targetCPA,
                                primaryGeo, seasonTags, analysable, thinData, baseline }) {

  const isLeadGen  = /lead/i.test(objective);
  const isTraffic  = /traffic|click/i.test(objective);
  const isSales    = !isLeadGen && !isTraffic;

  const baselineContext = baseline ? `
ACCOUNT BASELINE (${companyName}'s normal in ${currency}):
- ROAS median: ${baseline.roas_4w_median ?? 'n/a'} | p25: ${baseline.roas_4w_p25 ?? 'n/a'} | p75: ${baseline.roas_4w_p75 ?? 'n/a'}
- CPA median: ${baseline.cpa_4w_median ?? 'n/a'} ${currency}
- CTR median: ${baseline.ctr_4w_median ?? 'n/a'}%
- Trend: ${baseline.trend_direction ?? 'unknown'}` : '';

  const systemPrompt = `You are a senior Meta ads creative analyst at a digital marketing agency. You analyse ad creative performance data and identify what is working, what is not, and why — based strictly on the data provided. British English throughout.

## ROLE
Write for a media buyer who will act on this immediately, in language clear enough for a client to read. Be specific, reference actual numbers, and name actual ads. Never generalise.

## CURRENCY — NON-NEGOTIABLE
All analysis is in ${currency}. Never apply global benchmarks. Judge performance relative to:
1. This account's own baseline in ${currency}
2. Break-even ROAS${breakEvenROAS ? ` (${breakEvenROAS})` : ' (not set)'}
3. Target CPA${targetCPA ? ` (${targetCPA} ${currency})` : ' (not set)'}
4. AOV${aov ? ` (${aov} ${currency}) — CPA as % of AOV is the key signal` : ' (not set)'}

## OBJECTIVE — STRICT
${isLeadGen ? `LEAD GENERATION ACCOUNT.
- Evaluate on: CPL, lead volume, cost efficiency
- NEVER mention ROAS or purchases as metrics — they are irrelevant for this account
- A creative with zero purchases is NOT underperforming` :
isTraffic ? `TRAFFIC ACCOUNT.
- Evaluate on: CPC, CTR, click volume
- Do not flag low conversions as problems` :
`SALES / ECOMMERCE ACCOUNT.
- Evaluate on: ROAS, CPA, purchases, revenue
- CPA as % of AOV is the primary efficiency signal`}

## SPEND THRESHOLD
- Minimum ${MIN_SPEND_FOR_ANALYSIS} ${currency} spend before evaluating a creative
- Ads below this threshold = "Insufficient data" — do not guess at performance
- Do NOT recommend pausing ads below threshold — they haven't had a fair test yet

## FATIGUE FLAGS
- If fatigue_flag is set on an ad, flag it explicitly regardless of current performance
- Fatigued ads may still show good ROAS this week but will decline — act proactively

## SEASONAL CONTEXT
Current season: ${seasonTags}
${/ramadan|eid|bfcm|christmas|white_friday/i.test(seasonTags)
  ? `HIGH-COMPETITION SEASON: Reference it explicitly. Seasonal creative typically outperforms evergreen during this period.`
  : ''}

## VIDEO ANALYSIS
For video/reel/story formats, analyse retention data:
- p25 retention: hook effectiveness (below 40% = weak hook)
- p50 retention: mid-video engagement
- p75/p100: strong if high — ad is holding attention
- Thruplay: key metric for video completion

## OUTPUT FORMAT
Use plain text. No asterisks, no markdown, no emojis unless data shows they performed well. British English.
CRITICAL CURRENCY RULE: This account uses ${currency}. Write ALL monetary figures as numbers followed by ${currency}. NEVER use £, $, €, or any currency symbol under any circumstances. Example: 379 ${currency} not £379.

Format exactly:

SCALE THESE

1. [Ad name]
   Spend: X | ${isSales ? 'ROAS: X | Purchases: X' : isLeadGen ? 'Leads: X | CPL: X' : 'Clicks: X | CPC: X'}
   Why: [One specific sentence citing what's working — hook angle, copy style, or format]

[Up to 3 ads]


CONSIDER PAUSING

1. [Ad name]
   Spend: X | ${isSales ? 'ROAS: X | Purchases: X' : isLeadGen ? 'Leads: X | CPL: X' : 'Clicks: X | CPC: X'}
   Why: [One specific sentence on why — weak metric, fatigue flag, or poor retention]

[Up to 3 ads]


WATCH CLOSELY

1. [Ad name]
   Spend: X | ${isSales ? 'ROAS: X | Purchases: X' : isLeadGen ? 'Leads: X | CPL: X' : 'Clicks: X | CPC: X'}
   Why: [Promising but needs more data, or mixed signals]

[Up to 3 ads — omit section if none]


INSUFFICIENT DATA

[List any ads below ${MIN_SPEND_FOR_ANALYSIS} ${currency} spend — one line each with spend shown]


KEY INSIGHT

[One sentence identifying the single most important pattern across this week's creatives — what separates winners from losers]


RECOMMENDED ACTION

[One specific, immediately actionable next step for the media buyer — reference actual ad names and numbers]`;

  const adBlocks = analysable.map((c, i) => formatCreative(c, currency, i)).join('\n\n---\n\n');
  const thinBlock = thinData.length > 0
    ? `\n\nINSUFFICIENT SPEND (below ${MIN_SPEND_FOR_ANALYSIS} ${currency}):\n` +
      thinData.map(c => `- ${c.fields.ad_name || 'Unnamed'}: ${c.fields.spend ?? 0} ${currency} spend`).join('\n')
    : '';

  const userMessage = `Analyse Meta ad creatives for ${companyName} (${adAccountId}):

Week ending: ${weekEnding}
Currency: ${currency}
Objective: ${objective}
Geography: ${primaryGeo}
Season: ${seasonTags}
${aov ? `AOV: ${aov} ${currency}` : ''}
${breakEvenROAS ? `Break-even ROAS: ${breakEvenROAS}` : ''}
${targetCPA ? `Target CPA: ${targetCPA} ${currency}` : ''}
${baselineContext}

CREATIVES THIS WEEK:

${adBlocks}${thinBlock}`;

  return { systemPrompt, userMessage };
}

// ─── BUILD BRIEF GENERATION PROMPT ──────────────────────────────────────────

function buildBriefPrompt({ companyName, objective, currency, aov, seasonTags, seasonStatus, brandTone,
                             analysisText, clientProfile }) {

  const isLeadGen     = /lead/i.test(objective);
  const productType   = Array.isArray(clientProfile?.product_type)
                          ? clientProfile.product_type.join(', ')
                          : (clientProfile?.product_type || 'unknown');
  const targetAudience = Array.isArray(clientProfile?.target_audience)
                          ? clientProfile.target_audience.join(', ')
                          : (clientProfile?.target_audience || 'unknown');
  const creativeFormats = Array.isArray(clientProfile?.creative_formats)
                          ? clientProfile.creative_formats.join(', ')
                          : (clientProfile?.creative_formats || 'all formats');
  const offerType      = Array.isArray(clientProfile?.offer_type)
                          ? clientProfile.offer_type.join(', ')
                          : (clientProfile?.offer_type || 'unknown');
  const purchaseSpeed  = clientProfile?.purchase_speed || 'unknown';

  const systemPrompt = `You are a senior Meta ads creative strategist at a digital marketing agency. You translate performance analysis into three distinct, immediately actionable creative briefs for new Meta ads. British English throughout.

## YOUR ROLE
Write briefs that a designer and copywriter can execute without asking questions. Be specific, visual, and reference the performance data that supports each direction. Never invent angles not supported by the analysis.

## OBJECTIVE
${isLeadGen ? 'LEAD GENERATION — briefs should focus on capturing contact details. CTAs should drive form fills, messages, or calls.' : 'SALES / ECOMMERCE — briefs should drive purchase intent. CTAs should drive to product pages or checkout.'}

## CURRENCY
All numbers in ${currency}. Write figures as: 379 ${currency}. NEVER use £, $, €, or any currency symbol. British English does not mean British pounds — the currency is ${currency}.

## SEASONAL CONTEXT
Current season: ${seasonTags} (Status: ${seasonStatus || 'active'})
${seasonStatus === 'upcoming'
  ? `SEASON UPCOMING: ${seasonTags} is approaching. At least one brief should prepare seasonal creative ready to launch at season start.`
  : seasonStatus === 'active'
    ? (/ramadan|eid/i.test(seasonTags)
        ? `RAMADAN / EID WINDOW ACTIVE: This is one continuous campaign window — campaigns started in Ramadan carry through Eid al-Fitr. Write briefs that work across both periods. Urgency, generosity, celebration, and family themes perform strongly. At least one brief should leverage this seasonal context.`
        : `HIGH-COMPETITION SEASON ACTIVE: At least one brief should be season-specific with a seasonal hook and offer angle.`)
    : seasonStatus === 'recently_ended'
      ? `SEASON RECENTLY ENDED (within 2 weeks): Do NOT write new seasonal briefs — they cannot be produced and tested in time. Instead, identify what worked during the season (hook angles, offer types, urgency cues, creative formats) and adapt those principles to evergreen creative that will perform year-round. In why_it_works, explicitly name the seasonal learning you are adapting.`
      : ''}

## CLIENT CONTEXT
- Product type: ${productType}
- Target audience: ${targetAudience}
- Available formats: ${creativeFormats}
- Offer type: ${offerType}
- Purchase speed: ${purchaseSpeed}
${aov ? `- AOV: ${aov} ${currency}` : ''}

## BRAND TONE — THIS OVERRIDES ALL HOOK AND COPY DECISIONS
Brand tone: ${brandTone || 'warm'}

${brandTone === 'premium' || brandTone === 'luxury' || brandTone === 'minimal'
  ? `PREMIUM/LUXURY BRAND. This brand competes on exclusivity, quality, and aspiration. The creative must reflect that.
HOOK RULES: No wordplay. No puns. No exclamation marks. No casual language. Hooks should be restrained, elegant, and aspirational. Fewer words carry more weight. Think: 'For moments that matter.' / 'Nothing ordinary. Ever.' / 'The art of celebration.'
COPY RULES: Short sentences. Sophisticated vocabulary. No discount language. No urgency tactics. The brand speaks with quiet confidence, not excitement.
NEVER: puns, exclamation marks, emoji, casual slang, price anchoring, countdown urgency.`
  : brandTone === 'playful' || brandTone === 'fun'
    ? `PLAYFUL BRAND. This brand's personality is fun, energetic, and joyful. Creative should reflect that energy.
HOOK RULES: Wordplay and puns are strongly encouraged — especially for seasonal moments. Keep them clever and relevant to the product. Celebration, delight, and smiles are the goals.
COPY RULES: Conversational, upbeat, warm. Short punchy sentences. Emojis acceptable if data shows they performed well. Brand voice is a friend, not a corporation.`
  : brandTone === 'bold'
    ? `BOLD BRAND. Direct, confident, no-nonsense. Makes strong statements.
HOOK RULES: Short, punchy, declarative. No hedging. 'This changes everything.' / 'You've been doing it wrong.' / 'Stop settling.'
COPY RULES: Short sentences. Active voice. Strong verbs. No fluff.`
  : brandTone === 'professional'
    ? `PROFESSIONAL BRAND. Credible, authoritative, trustworthy. Often B2B or considered-purchase categories.
HOOK RULES: Lead with credibility, results, or expertise. No puns. No casual language. 'The only solution your team needs.' / 'Trusted by 500 companies across the UAE.'
COPY RULES: Clear, factual, benefit-focused. Professional but not cold.`
  : brandTone === 'aspirational'
    ? `ASPIRATIONAL BRAND. Sits between warm and premium — lifestyle, mid-tier fashion, travel, home décor, wellness. The brand sells an elevated version of the audience's life.
HOOK RULES: Paint a picture of the life they want. Hooks should make people think 'I want to feel like that.' No puns. No hard sell. 'This is the life.' / 'You deserve this.' / 'The version of you that has this.' / 'Some things are worth it.'
COPY RULES: Evocative, sensory language. Paint scenes not features. Short sentences that breathe. Confident but not cold.`
  : brandTone === 'cultural'
    ? `CULTURAL BRAND. Community, heritage, tradition, and shared identity are central to this brand. Often relevant for Gulf market brands with deep cultural roots or brands targeting specific communities.
HOOK RULES: Speak to shared values, traditions, and moments of collective identity. Hooks should feel like they come from inside the community. 'For the family that celebrates together.' / 'Eid is more than a day. It's us.' / 'Rooted in tradition. Made for today.'
COPY RULES: Warm, inclusive, community-first language. Reference shared cultural moments authentically. Never appropriative — must be genuinely relevant to the brand's audience.`
  : brandTone === 'urgent' || brandTone === 'urgent/direct'
    ? `URGENT/DIRECT BRAND. Performance-first. Every word earns its place. Drives action immediately. Often used for ecommerce flash sales, limited offers, lead generation, or high-conversion campaigns.
HOOK RULES: Lead with the offer, the deadline, or the consequence of not acting. No fluff. 'Ends tonight.' / 'Only 12 left.' / 'Last chance to get this price.' / 'Your competitors are already using this.'
COPY RULES: Short. Direct. One idea per sentence. Strong CTA. Numbers where possible. Urgency must feel real — fabricated urgency destroys trust.`
  : `WARM/ACCESSIBLE BRAND. Friendly, human, relatable. The majority of lifestyle and consumer brands.
HOOK RULES: Emotional connection, aspiration, identity, and occasion-based hooks work well. Subtle wordplay is acceptable when brand-relevant — not forced puns. Celebratory and joyful energy.
COPY RULES: Conversational but polished. Warm tone. Speaks to the person, not at them.`}

## RULES
- Generate exactly 3 briefs
- Each brief must take a distinct creative direction — no two can share the same hook style or visual approach
- Base every direction on insights from the analysis — cite the specific insight that supports it
- If the analysis flags something as underperforming, do NOT use that approach
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
      "visual_concept": "What the viewer sees — scene, style, motion, framing. Be specific. Base on image_tags and visual data from top-performing ads.",
      "copy_body": "Main ad text — max 125 characters",
      "cta": "Button label and optional supporting caption",
      "format_adaptations": "Feed (1080x1080): how visual frames in square. Stories/Reels (1080x1920): how it reframes for vertical.",
      "why_it_works": "Cite the specific line from the performance analysis above that supports this direction. Must reference actual ad names or metrics."
    },
    { "same structure for brief 2" },
    { "same structure for brief 3" }
  ]
}

RULES:
- Exactly 3 objects in the briefs array — no more, no less
- why_it_works must quote or directly reference a specific finding from the analysis — no generic statements
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

  const userMessage = `Based on this creative performance analysis for ${companyName}, generate three creative briefs:

${analysisText}`;

  return { systemPrompt, userMessage };
}

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
                               analysisText, briefText, briefs }, env) {

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
