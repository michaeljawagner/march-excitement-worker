export default {
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(runSnapshotJob(env));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    if (url.pathname === "/health") {
      return json({
        ok: true,
        hasKV: !!env.MARCH_DB,
        hasProxyBase: !!env.PROXY_BASE
      });
    }

    if (url.pathname === "/run") {
      await runSnapshotJob(env);
      return json({ ok: true, ran: true });
    }

    if (url.pathname === "/summary") {
      const gameId = String(url.searchParams.get("gameId") || "").trim();
      if (!gameId) return json({ ok: false, error: "Missing gameId" }, 400);

      const summary = await env.MARCH_DB.get("summary:" + gameId, "json");
      if (!summary) return json({ ok: false, error: "Not found" }, 404);

      return json({ ok: true, summary });
    }

    if (url.pathname === "/history") {
      const gameId = String(url.searchParams.get("gameId") || "").trim();
      if (!gameId) return json({ ok: false, error: "Missing gameId" }, 400);

      const history = await env.MARCH_DB.get("hist:" + gameId, "json");
      if (!history) return json({ ok: false, error: "Not found" }, 404);

      return json({ ok: true, history });
    }

    return json({ ok: false, error: "Not found" }, 404);
  }
};

async function runSnapshotJob(env) {
  if (!env.MARCH_DB) throw new Error("Missing MARCH_DB binding");
  if (!env.PROXY_BASE) throw new Error("Missing PROXY_BASE var");

  const tournamentDates = [
    "20260317","20260318","20260319","20260320","20260321","20260322",
    "20260326","20260327","20260328","20260329","20260404","20260406"
  ];

  for (const scoreboardDate of tournamentDates) {
    const games = await fetchEspnGames(env, scoreboardDate);

    for (const game of games) {
      const phase = getGamePhase(game.game);

      if (phase === "upcoming") continue;

      if (phase === "live") {
        await logLiveSnapshot(env, game);
        continue;
      }

      if (phase === "final") {
        await finalizeGame(env, game);
      }
    }
  }
}

async function fetchEspnGames(env, scoreboardDate) {
  const url =
    "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard" +
    "?dates=" + scoreboardDate +
    "&groups=50&limit=365";

  const data = await fetchWithProxy(env, url);
  const events = Array.isArray(data?.events) ? data.events : [];

  return events
    .map((game) => {
      const comp = game?.competitions?.[0];
      const competitors = comp?.competitors || [];
      if (competitors.length < 2) return null;

      const t1 = competitors[0];
      const t2 = competitors[1];
      const team1 = getTeamName(t1.team);
      const team2 = getTeamName(t2.team);

      if (!team1 || !team2) return null;

      const round = getMarchMadnessRound(game);
      if (!round) return null;
      if (!isMensMarchMadnessGame(game)) return null;

      return {
        espnId: String(game.id),
        scoreboardDate,
        round,
        game,
        team1,
        team2
      };
    })
    .filter(Boolean);
}

async function logLiveSnapshot(env, gameInfo) {
  const marketData = await fetchMarketForGame(env, gameInfo);
  if (!marketData) return;

  const comp = gameInfo.game?.competitions?.[0];
  const competitors = comp?.competitors || [];
  if (competitors.length < 2) return;

  const t1 = competitors[0];
  const t2 = competitors[1];

  const scoreA = Number(t1?.score || 0);
  const scoreB = Number(t2?.score || 0);
  const probA = Number(marketData.probA);
  const probB = Number(marketData.probB);

  const excitement = getExcitementScoreFromHistory(
    marketData.history,
    {
      seedOrange: getTeamSeedNumber(t1),
      seedBlue: getTeamSeedNumber(t2)
    }
  );

  const key = "hist:" + gameInfo.espnId;
  const existing = await env.MARCH_DB.get(key, "json");
  const snapshots = Array.isArray(existing?.snapshots) ? existing.snapshots : [];

  const latestTs = marketData.history?.length
    ? Number(marketData.history[marketData.history.length - 1].t)
    : Math.floor(Date.now() / 1000);

  if (snapshots.length && Number(snapshots[snapshots.length - 1]?.historyTs) === latestTs) {
    return;
  }

  snapshots.push({
    ts: Date.now(),
    historyTs: latestTs,
    status: "live",
    round: gameInfo.round,
    scoreboardDate: gameInfo.scoreboardDate,
    teamA: gameInfo.team1,
    teamB: gameInfo.team2,
    scoreA,
    scoreB,
    probA,
    probB,
    excitement
  });

  await env.MARCH_DB.put(
    key,
    JSON.stringify({
      gameId: gameInfo.espnId,
      teamA: gameInfo.team1,
      teamB: gameInfo.team2,
      round: gameInfo.round,
      snapshots
    })
  );
}

async function finalizeGame(env, gameInfo) {
  const summaryKey = "summary:" + gameInfo.espnId;
  const existingSummary = await env.MARCH_DB.get(summaryKey, "json");
  if (existingSummary?.finalized) return;

  const histKey = "hist:" + gameInfo.espnId;
  const historyDoc = await env.MARCH_DB.get(histKey, "json");
  let snapshots = Array.isArray(historyDoc?.snapshots) ? historyDoc.snapshots : [];

  const comp = gameInfo.game?.competitions?.[0];
  const competitors = comp?.competitors || [];
  if (competitors.length < 2) return;

  const t1 = competitors[0];
  const t2 = competitors[1];

  const finalScoreA = Number(t1?.score || 0);
  const finalScoreB = Number(t2?.score || 0);

  // Fallback: if no live snapshots were captured, try one last market read
  if (!snapshots.length) {
    const marketData = await fetchMarketForGame(env, gameInfo);
    if (marketData?.history?.length) {
      snapshots = [{
        ts: Date.now(),
        historyTs: Number(marketData.history[marketData.history.length - 1].t) || Math.floor(Date.now() / 1000),
        status: "final",
        round: gameInfo.round,
        scoreboardDate: gameInfo.scoreboardDate,
        teamA: gameInfo.team1,
        teamB: gameInfo.team2,
        scoreA: finalScoreA,
        scoreB: finalScoreB,
        probA: Number(marketData.probA),
        probB: Number(marketData.probB),
        excitement: getExcitementScoreFromHistory(
          marketData.history,
          {
            seedOrange: getTeamSeedNumber(t1),
            seedBlue: getTeamSeedNumber(t2)
          }
        )
      }];

      await env.MARCH_DB.put(
        histKey,
        JSON.stringify({
          gameId: gameInfo.espnId,
          teamA: gameInfo.team1,
          teamB: gameInfo.team2,
          round: gameInfo.round,
          snapshots
        })
      );
    }
  }

  let peakExcitement = 0;
  let totalExcitement = 0;
  let flipCount = 0;
  let maxSwing = 0;

  for (let i = 0; i < snapshots.length; i++) {
    const s = snapshots[i];
    const exc = Number(s.excitement || 0);
    peakExcitement = Math.max(peakExcitement, exc);
    totalExcitement += exc;

    if (i > 0) {
      const prev = snapshots[i - 1];
      const swing = Math.abs(Number(s.probA || 0) - Number(prev.probA || 0));
      if (swing > maxSwing) maxSwing = swing;

      const prevFavA = Number(prev.probA || 0) >= 0.5;
      const currFavA = Number(s.probA || 0) >= 0.5;
      if (prevFavA !== currFavA) flipCount++;
    }
  }

  const avgExcitement = snapshots.length ? totalExcitement / snapshots.length : 0;
  const finalExcitement = snapshots.length
    ? Number(snapshots[snapshots.length - 1]?.excitement || 0)
    : 0;

  const summary = {
    finalized: true,
    gameId: gameInfo.espnId,
    round: gameInfo.round,
    teamA: gameInfo.team1,
    teamB: gameInfo.team2,
    finalScoreA,
    finalScoreB,
    finalExcitement: Number(finalExcitement.toFixed(1)),
    peakExcitement: Number(peakExcitement.toFixed(1)),
    avgExcitement: Number(avgExcitement.toFixed(1)),
    flipCount,
    maxSwing: Number(maxSwing.toFixed(3)),
    snapshotCount: snapshots.length,
    finalizedAt: Date.now()
  };

  await env.MARCH_DB.put(summaryKey, JSON.stringify(summary));
}

async function fetchMarketForGame(env, gameInfo) {
  const slugA = getPolySlug(gameInfo.team1);
  const slugB = getPolySlug(gameInfo.team2);
  if (!slugA || !slugB) return null;

  const baseDate = gameInfo.scoreboardDate;
  const dateCandidates = [baseDate, shiftYmd(baseDate, -1), shiftYmd(baseDate, 1)];

  const slugs = [];
  const seen = new Set();

  for (const ymd of dateCandidates) {
    const dateSlug = ymd.slice(0, 4) + "-" + ymd.slice(4, 6) + "-" + ymd.slice(6, 8);
    const candidates = [
      "cbb-" + slugA + "-" + slugB + "-" + dateSlug,
      "cbb-" + slugB + "-" + slugA + "-" + dateSlug
    ];
    for (const slug of candidates) {
      if (!seen.has(slug)) {
        seen.add(slug);
        slugs.push(slug);
      }
    }
  }

  for (const slug of slugs) {
    try {
      const eventData = await fetchWithProxy(env, "https://gamma-api.polymarket.com/events/slug/" + slug);
      if (!eventData?.slug) continue;

      const market = primaryGameMarket(eventData.markets || [], gameInfo);
      if (!market) continue;

      const outcomes = parseMaybeJson(market.outcomes) || [gameInfo.team1, gameInfo.team2];
      const tokenIds = parseMaybeJson(market.clobTokenIds);
      const prices = parseMaybeJson(market.outcomePrices);

      if (
        !Array.isArray(outcomes) || outcomes.length < 2 ||
        !Array.isArray(tokenIds) || tokenIds.length < 2 ||
        !Array.isArray(prices) || prices.length < 2
      ) continue;

      const teamAIndex = findOutcomeIndex(outcomes, gameInfo.team1);
      const teamBIndex = findOutcomeIndex(outcomes, gameInfo.team2);
      if (teamAIndex < 0 || teamBIndex < 0 || teamAIndex === teamBIndex) continue;

      const startTs = Math.floor(
        new Date(market.gameStartTime || eventData.startDate || gameInfo.game.date).getTime() / 1000
      );
      const endTs = Math.floor(Date.now() / 1000);

      const historyRes = await fetchWithProxy(
        env,
        "https://clob.polymarket.com/prices-history?market=" +
          encodeURIComponent(tokenIds[teamAIndex]) +
          "&startTs=" + startTs +
          "&endTs=" + endTs +
          "&fidelity=0.5"
      );

      const history = Array.isArray(historyRes?.history) ? trimHistoryAtResolution(historyRes.history) : [];
      if (!history.length) continue;

      const latestProb = Number(history[history.length - 1].p);
      if (!Number.isFinite(latestProb)) continue;

      return {
        probA: latestProb,
        probB: 1 - latestProb,
        history
      };
    } catch (e) {}
  }

  return null;
}

async function fetchWithProxy(env, url) {
  const proxyBase = String(env.PROXY_BASE || "");
  if (!proxyBase) throw new Error("Missing PROXY_BASE");
  const res = await fetch(proxyBase + encodeURIComponent(url));
  if (!res.ok) throw new Error("Fetch failed: " + res.status);

  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON response");
  }
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: corsHeaders()
  });
}

function corsHeaders() {
  return {
    "content-type": "application/json",
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "Content-Type"
  };
}

function parseMaybeJson(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

function getTeamName(team) {
  return team?.displayName || team?.shortDisplayName || team?.name || "";
}

function getGamePhase(game) {
  const state = game?.status?.type?.state;
  if (state === "post") return "final";
  if (state === "in") return "live";
  return "upcoming";
}

function normalizeTeamLookup(str) {
  return String(str || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’']/g, "")
    .replace(/&/g, "and")
    .replace(/\./g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalize(str) {
  return String(str || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/'/g, "")
    .replace(/\bsaint\b/g, "st")
    .replace(/\bpennsylvania\b/g, "penn")
    .replace(/\b(university|college|state|st\.|rainbow warriors|anteaters|wildcats|tigers|bulldogs|warriors|cougars|bears|hawks|eagles|knights|panthers|bruins|trojans|huskies|gaels|boilermakers|tar heels|blue devils|cavaliers|volunteers|aggies|mustangs|owls|cardinals|badgers|cyclones|seminoles|gators|rebels|raiders|spartans|wolfpack|mountaineers|lobos|illini|hoosiers|hurricanes|crimson tide|longhorns|bearcats|terrapins|horned frogs|zags|pirates|beavers|lancers|wolverines|quakers|flyers|rams|commodores|razorbacks)\b/g, "")
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function isMensMarchMadnessGame(game) {
  const text = extractTournamentText(game);
  return (
    text.includes("ncaa tournament") ||
    text.includes("march madness") ||
    text.includes("first four") ||
    text.includes("final four") ||
    text.includes("sweet 16") ||
    text.includes("sweet sixteen") ||
    text.includes("elite eight") ||
    text.includes("elite 8") ||
    text.includes("national championship")
  );
}

function extractTournamentText(game) {
  const pieces = [
    game.name || "",
    game.shortName || "",
    game.season?.slug || "",
    game.season?.type?.name || "",
    game.league?.name || "",
    game.status?.type?.description || "",
    game.status?.type?.detail || "",
    game.status?.type?.shortDetail || ""
  ];

  if (Array.isArray(game.competitions)) {
    game.competitions.forEach(function (comp) {
      pieces.push(comp.note || "", comp.headline || "", comp.type?.text || "");
      if (Array.isArray(comp.notes)) {
        comp.notes.forEach(function (n) {
          pieces.push(n?.headline || "", n?.text || "");
        });
      }
    });
  }

  return pieces.join(" ").toLowerCase();
}

function getMarchMadnessRound(game) {
  const text = extractTournamentText(game);
  if (text.includes("first four")) return "First Four";
  if (text.includes("round of 64") || text.includes("first round")) return "Round of 64";
  if (text.includes("round of 32") || text.includes("second round")) return "Round of 32";
  if (text.includes("sweet sixteen") || text.includes("sweet 16")) return "Sweet Sixteen";
  if (text.includes("elite eight") || text.includes("elite 8")) return "Elite Eight";
  if (text.includes("final four")) return "Final Four";
  if (text.includes("national championship") || text.includes("championship game")) return "National Championship";

  const ymd = formatDateKey(new Date(game.date));
  const map = {
    "20260317":"First Four",
    "20260318":"First Four",
    "20260319":"Round of 64",
    "20260320":"Round of 64",
    "20260321":"Round of 32",
    "20260322":"Round of 32",
    "20260326":"Sweet Sixteen",
    "20260327":"Sweet Sixteen",
    "20260328":"Elite Eight",
    "20260329":"Elite Eight",
    "20260404":"Final Four",
    "20260406":"National Championship"
  };

  return map[ymd] || null;
}

function getTeamSeedNumber(competitor) {
  const seed =
    competitor?.curatedRank?.current ??
    competitor?.seed ??
    competitor?.tournamentSeed ??
    null;
  const num = Number(seed);
  return Number.isFinite(num) ? num : null;
}

function getSeedGapBonus(seedOrange, seedBlue, finalProbOrange, probs) {
  if (!Number.isFinite(seedOrange) || !Number.isFinite(seedBlue)) return 0;

  const biggerSeed = Math.max(seedOrange, seedBlue);
  const smallerSeed = Math.min(seedOrange, seedBlue);
  const gap = biggerSeed - smallerSeed;
  if (gap < 4) return 0;

  const orangeIsUnderdogBySeed = seedOrange > seedBlue;
  const underdogLateProb = probs.length
    ? (orangeIsUnderdogBySeed ? finalProbOrange : (1 - finalProbOrange))
    : 0;

  let bonus = 0;
  if (gap >= 10) bonus += 0.5;
  else if (gap >= 7) bonus += 0.3;
  else bonus += 0.15;

  if (underdogLateProb >= 0.25) bonus += 0.2;
  if (underdogLateProb >= 0.40) bonus += 0.35;
  if (underdogLateProb >= 0.50) bonus += 0.45;

  if (underdogLateProb >= 0.5) {
    if (gap >= 10) bonus += 1.0;
    else if (gap >= 7) bonus += 0.75;
    else bonus += 0.45;
  }

  return bonus;
}

function getExcitementScoreFromHistory(history, state) {
  if (!Array.isArray(history) || history.length < 8) return 2.5;

  const probs = history.map(p => Number(p.p)).filter(Number.isFinite);
  if (probs.length < 8) return 2.5;

  let totalMove = 0;
  let closeCount = 0;
  let flips = 0;

  for (let i = 1; i < probs.length; i++) {
    const prev = probs[i - 1];
    const curr = probs[i];
    totalMove += Math.abs(curr - prev);
    if ((prev >= 0.5) !== (curr >= 0.5)) flips++;
    if (curr >= 0.43 && curr <= 0.57) closeCount++;
  }

  const lateStart = Math.floor(probs.length * 0.8);
  let lateMove = 0;
  let lateFlips = 0;

  for (let i = lateStart + 1; i < probs.length; i++) {
    lateMove += Math.abs(probs[i] - probs[i - 1]);
    if ((probs[i - 1] >= 0.5) !== (probs[i] >= 0.5)) lateFlips++;
  }

  const closenessRatio = closeCount / probs.length;
  let raw =
    (totalMove * 1.4) +
    (closenessRatio * 1.8) +
    (lateMove * 2.3) +
    (flips * 0.12) +
    (lateFlips * 0.45);

  const firstProb = probs[0];
  const finalProb = probs[probs.length - 1];

  if (firstProb >= 0.60 && finalProb < 0.50) raw += 0.8;
  raw += getSeedGapBonus(Number(state.seedOrange), Number(state.seedBlue), finalProb, probs);

  if (closenessRatio < 0.10 && flips === 0) raw -= 0.6;
  if (Math.max.apply(null, probs) >= 0.96 || Math.min.apply(null, probs) <= 0.04) raw -= 0.35;

  return Number(clamp(raw, 0, 9.7).toFixed(1));
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function trimHistoryAtResolution(history) {
  if (!Array.isArray(history) || history.length < 2) return history;

  const EPSILON = 0.0005;
  const lastProb = Number(history[history.length - 1].p);
  if (!Number.isFinite(lastProb)) return history;
  if (!(lastProb >= 0.999 || lastProb <= 0.001)) return history;

  let plateauStart = history.length - 1;
  for (let i = history.length - 2; i >= 0; i--) {
    const prob = Number(history[i].p);
    if (!Number.isFinite(prob)) break;
    if (Math.abs(prob - lastProb) > EPSILON) break;
    plateauStart = i;
  }

  return plateauStart === history.length - 1 ? history : history.slice(0, plateauStart + 1);
}

function primaryGameMarket(markets, espnGame) {
  const list = Array.isArray(markets) ? markets : [];

  function key(str) {
    return normalizeTeamLookup(str)
      .replace(/^north carolina state$/, "nc state")
      .replace(/^north carolina state wolfpack$/, "nc state")
      .replace(/^connecticut$/, "uconn")
      .replace(/^connecticut huskies$/, "uconn")
      .replace(/^pennsylvania$/, "penn")
      .replace(/^pennsylvania quakers$/, "penn")
      .replace(/^queens university$/, "queens")
      .replace(/^queens university royals$/, "queens")
      .replace(/^long island university$/, "liu")
      .replace(/^long island university sharks$/, "liu")
      .replace(/^mcneese state$/, "mcneese")
      .replace(/^mcneese state cowboys$/, "mcneese");
  }

  const teamA = key(espnGame?.team1 || "");
  const teamB = key(espnGame?.team2 || "");

  function looksLikeTeamMarket(m) {
    const outcomes = parseMaybeJson(m.outcomes);
    const prices = parseMaybeJson(m.outcomePrices);
    if (!Array.isArray(outcomes) || outcomes.length !== 2) return false;
    if (!Array.isArray(prices) || prices.length !== 2) return false;

    const o0 = String(outcomes[0] || "").trim();
    const o1 = String(outcomes[1] || "").trim();
    const k0 = key(o0);
    const k1 = key(o1);

    if (!k0 || !k1) return false;
    if (/\byes\b|\bno\b|\bover\b|\bunder\b/i.test(o0 + " " + o1)) return false;

    const aMatch =
      k0 === teamA || k1 === teamA || k0.includes(teamA) || k1.includes(teamA) || teamA.includes(k0) || teamA.includes(k1);

    const bMatch =
      k0 === teamB || k1 === teamB || k0.includes(teamB) || k1.includes(teamB) || teamB.includes(k0) || teamB.includes(k1);

    return aMatch && bMatch;
  }

  const valid = list.filter(looksLikeTeamMarket);
  valid.sort((a, b) => Number(b.volume || 0) - Number(a.volume || 0));
  return valid[0] || null;
}

function findOutcomeIndex(outcomes, teamName) {
  function key(str) {
    return normalizeTeamLookup(str)
      .replace(/^north carolina state$/, "nc state")
      .replace(/^north carolina state wolfpack$/, "nc state")
      .replace(/^connecticut$/, "uconn")
      .replace(/^connecticut huskies$/, "uconn")
      .replace(/^pennsylvania$/, "penn")
      .replace(/^pennsylvania quakers$/, "penn")
      .replace(/^queens university$/, "queens")
      .replace(/^queens university royals$/, "queens")
      .replace(/^long island university$/, "liu")
      .replace(/^long island university sharks$/, "liu")
      .replace(/^mcneese state$/, "mcneese")
      .replace(/^mcneese state cowboys$/, "mcneese");
  }

  const teamKey = key(teamName);

  for (let i = 0; i < outcomes.length; i++) {
    const k = key(outcomes[i]);
    if (k === teamKey || k.includes(teamKey) || teamKey.includes(k)) return i;
  }

  return -1;
}

function getPolySlug(name) {
  const lookup = {
    "howard": "howrd",
    "umbc": "umbc",
    "nc state": "ncst",
    "nc st": "ncst",
    "north carolina state": "ncst",
    "texas": "tx",
    "lehigh": "lehi",
    "prairie view aandm": "pvam",
    "smu": "smu",
    "miami oh": "miaoh",
    "tcu": "tcu",
    "ohio state": "ohiost",
    "troy": "troy",
    "nebraska": "nebr",
    "south florida": "sfl",
    "usf": "sfl",
    "louisville": "lou",
    "high point": "hpnt",
    "wisconsin": "wisc",
    "siena": "siena",
    "duke": "duke",
    "mcneese": "mcnst",
    "mcneese state": "mcnst",
    "vanderbilt": "vand",
    "north dakota state": "ndkst",
    "michigan state": "mst",
    "hawaii": "hawaii",
    "arkansas": "ark",
    "vcu": "vcu",
    "north carolina": "ncar",
    "texas aandm": "txam",
    "saint marys": "stmry",
    "st marys": "stmry",
    "penn": "penn",
    "pennsylvania": "penn",
    "illinois": "ill",
    "saint louis": "stlou",
    "st louis": "stlou",
    "georgia": "ga",
    "kennesaw state": "kenest",
    "gonzaga": "gnzg",
    "idaho": "idaho",
    "houston": "hou",
    "santa clara": "sanclr",
    "kentucky": "uk",
    "akron": "akron",
    "texas tech": "txtech",
    "liu": "liub",
    "long island university": "liub",
    "arizona": "arz",
    "virginia": "vir",
    "wright state": "wrght",
    "tennessee state": "tenst",
    "iowa state": "iowast",
    "alabama": "ala",
    "hofstra": "hofst",
    "villanova": "vill",
    "utah state": "utahst",
    "iowa": "iowa",
    "clemson": "clmsn",
    "northern iowa": "niowa",
    "st johns": "stjohn",
    "saint johns": "stjohn",
    "ucf": "ucf",
    "ucla": "ucla",
    "purdue": "pur",
    "queens": "queen",
    "queens university": "queen",
    "california baptist": "cabap",
    "cal baptist": "cabap",
    "kansas": "kan",
    "furman": "furman",
    "uconn": "uconn",
    "connecticut": "uconn",
    "miami": "mia",
    "missouri": "missr"
  };

  const key = normalizeTeamLookup(name);
  return lookup[key] || null;
}

function shiftYmd(yyyymmdd, deltaDays) {
  const y = Number(yyyymmdd.slice(0, 4));
  const m = Number(yyyymmdd.slice(4, 6));
  const d = Number(yyyymmdd.slice(6, 8));
  const dt = new Date(y, m - 1, d);
  dt.setDate(dt.getDate() + deltaDays);
  return formatDateKey(dt);
}

function formatDateKey(d) {
  const pad2 = (n) => String(n).padStart(2, "0");
  return d.getFullYear().toString() + pad2(d.getMonth() + 1) + pad2(d.getDate());
}
