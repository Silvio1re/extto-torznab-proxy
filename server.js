const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || "8998");
const CONFIG_DIR = process.env.CONFIG_DIR || "/config";
const CONFIG_FILE = path.join(CONFIG_DIR, "config.json");

const DEFAULT_CONFIG = {
  baseUrl: "https://extranet.torrentbay.st",
  publicUrl: "",
  flaresolverrUrl: "http://flaresolverr:8191/v1",
  sessionName: "extto_persistent",
  apiKey: "",
  withAdult: true,
  warmEnabled: true,
  warmQuery: "warm",
  warmIntervalMinutes: 10,
  coldThresholdSeconds: 30,
  requestTimeoutMs: 120000,
  idleTimeoutMinutes: 360,
  searchTtlSeconds: 180,
  magnetTtlMinutes: 1440,
  maxSearchCacheItems: 100,
  maxMagnetCacheItems: 1000,
  stripSeasonFromSearch: true
};

let config = loadConfig();

let sessionReady = false;
let sessionBusy = false;
let fsBusy = false;
let lastUsed = Date.now();
let lastWarm = null;
let currentWarm = null;

const searchCache = new Map();
const magnetCache = new Map();

let sessionQueue = Promise.resolve();
let fsQueue = Promise.resolve();

function nowIso() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(nowIso(), ...args);
}

function warn(...args) {
  console.warn(nowIso(), ...args);
}

function error(...args) {
  console.error(nowIso(), ...args);
}

function ensureConfigDir() {
  fs.mkdirSync(CONFIG_DIR, { recursive: true });
}

function randomApiKey() {
  return crypto.randomBytes(18).toString("hex");
}

function loadConfig() {
  ensureConfigDir();

  if (!fs.existsSync(CONFIG_FILE)) {
    const initial = { ...DEFAULT_CONFIG, apiKey: randomApiKey() };
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(initial, null, 2));
    return initial;
  }

  try {
    const raw = fs.readFileSync(CONFIG_FILE, "utf8");
    const parsed = JSON.parse(raw);
    const merged = { ...DEFAULT_CONFIG, ...parsed };

    if (!merged.apiKey) {
      merged.apiKey = randomApiKey();
      fs.writeFileSync(CONFIG_FILE, JSON.stringify(merged, null, 2));
    }

    return merged;
  } catch (e) {
    error("[config] failed to read config, using defaults:", e.message || e);
    return { ...DEFAULT_CONFIG, apiKey: randomApiKey() };
  }
}

function saveConfig(next) {
  ensureConfigDir();
  config = { ...config, ...next };
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
}

function normalizeBaseUrl(s) {
  return String(s || "").replace(/\/+$/, "");
}

// ─── SANITIZE MAGNET: URL-encodira vrijednosti parametara (čuva naziv torrenta) ───
function sanitizeMagnet(uri) {
  if (!uri || typeof uri !== "string" || !uri.startsWith("magnet:")) return uri;
  const qIndex = uri.indexOf("?");
  if (qIndex === -1) return uri;
  const base = uri.substring(0, qIndex);
  const params = uri.substring(qIndex + 1).split("&");
  const encoded = params.map(p => {
    const eq = p.indexOf("=");
    if (eq === -1) return p;
    const k = p.substring(0, eq);
    let v = p.substring(eq + 1);
    if (k === "xt" || k === "x.pe") return k + "=" + v;
    if (!/%[0-9A-Fa-f]{2}/.test(v)) {
      try {
        v = encodeURIComponent(decodeURIComponent(v));
      } catch (e) {
        v = encodeURIComponent(v);
      }
    }
    return k + "=" + v;
  });
  return base + "?" + encoded.join("&");
}
// ─── END SANITIZE ───

function xmlEscape(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function htmlEscape(s) {
  return xmlEscape(s).replace(/'/g, "&#39;");
}

function decodeEntities(s) {
  return String(s || "")
    .replace(/&nbsp;/g, " ")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&quot;/g, "\"")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripTags(s) {
  return decodeEntities(String(s || "").replace(/<script[\s\S]*?<\/script>/gi, "").replace(/<style[\s\S]*?<\/style>/gi, "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());
}

function pick(re, s) {
  const m = String(s || "").match(re);
  return m ? m[1] : "";
}

function withSessionLock(fn) {
  const run = sessionQueue.then(fn, fn);
  sessionBusy = true;
  sessionQueue = run.catch(() => {}).finally(() => { sessionBusy = false; });
  return run;
}

function withFsLock(fn) {
  const run = fsQueue.then(fn, fn);
  fsBusy = true;
  fsQueue = run.catch(() => {}).finally(() => { fsBusy = false; });
  return run;
}

async function fsCall(payload) {
  const url = config.flaresolverrUrl;

  const r = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  const text = await r.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error("FlareSolverr bad JSON response: " + text.slice(0, 300));
  }

  if (!r.ok) {
    throw new Error("FlareSolverr HTTP " + r.status + ": " + text.slice(0, 300));
  }

  if (json.status && json.status !== "ok") {
    throw new Error("FlareSolverr status " + json.status + ": " + (json.message || ""));
  }

  return json;
}

async function destroySessionUnlocked() {
  try {
    await fsCall({ cmd: "sessions.destroy", session: config.sessionName });
  } catch (e) {
    warn("[session] destroy ignored:", e.message || e);
  }
  sessionReady = false;
}

async function createSessionUnlocked() {
  await fsCall({ cmd: "sessions.create", session: config.sessionName });
  sessionReady = true;
  lastUsed = Date.now();
  log("[session] created", config.sessionName);
}

async function ensureSessionUnlocked() {
  if (sessionReady) return;
  await destroySessionUnlocked();
  await createSessionUnlocked();
}

async function ensureSession() {
  return await withSessionLock(async () => {
    await ensureSessionUnlocked();
  });
}

async function resetSession() {
  return await withSessionLock(async () => {
    log("[session] reset");
    await destroySessionUnlocked();
    await createSessionUnlocked();
  });
}

function getUserAgent(solution) {
  return (
    solution &&
    (
      solution.userAgent ||
      solution.user_agent ||
      solution.headers?.["user-agent"] ||
      solution.headers?.["User-Agent"]
    )
  ) || "Mozilla/5.0";
}

async function fetchHtmlViaFlaresolverr(targetUrl) {
  await ensureSession();
  lastUsed = Date.now();

  let pageRes;

  try {
    pageRes = await fsCall({
      cmd: "request.get",
      url: targetUrl,
      session: config.sessionName,
      maxTimeout: Number(config.requestTimeoutMs || 120000)
    });
  } catch (e) {
    warn("[retry] request.get failed, resetting session:", e.message || e);
    await resetSession();
    pageRes = await fsCall({
      cmd: "request.get",
      url: targetUrl,
      session: config.sessionName,
      maxTimeout: Number(config.requestTimeoutMs || 120000)
    });
  }

  const html = pageRes && pageRes.solution ? pageRes.solution.response : "";
  if (!html) throw new Error("empty html response");

  return { html, solution: pageRes.solution || {} };
}

function setLimitedCache(map, key, value, maxItems, ttlMs) {
  if (map.size >= maxItems) {
    let oldestKey = null;
    let oldestTime = Infinity;
    for (const [k, v] of map.entries()) {
      const t = v.lastHitAt || v.createdAt || 0;
      if (t < oldestTime) { oldestTime = t; oldestKey = k; }
    }
    if (oldestKey) map.delete(oldestKey);
  }

  const now = Date.now();
  map.set(key, { value, createdAt: now, lastHitAt: now, expiresAt: now + ttlMs, hits: 0 });
}

function getCache(map, key) {
  const item = map.get(key);
  if (!item) return null;
  if (Date.now() > item.expiresAt) { map.delete(key); return null; }
  item.hits++;
  item.lastHitAt = Date.now();
  return item.value;
}

function clearExpiredCache() {
  const now = Date.now();
  let removedSearches = 0;
  let removedMagnets = 0;
  for (const [key, value] of searchCache.entries()) {
    if (now > value.expiresAt) { searchCache.delete(key); removedSearches++; }
  }
  for (const [key, value] of magnetCache.entries()) {
    if (now > value.expiresAt) { magnetCache.delete(key); removedMagnets++; }
  }
  return { removedSearches, removedMagnets };
}

function padNumberText(value, width) {
  const n = String(value || "").replace(/\D+/g, "");
  if (!n) return "";
  return n.padStart(width, "0");
}

function buildTvToken(params) {
  const season = padNumberText(params.get("season"), 2);
  const ep = padNumberText(params.get("ep") || params.get("episode"), 2);
  if (season && ep) return `S${season}E${ep}`;
  if (season) return `S${season}`;
  return "";
}

function normalizeSearchQueryText(q) {
  return String(q || "")
    .replace(/\s*:\s*/g, " ")
    .replace(/\bSeason\s*(\d{1,2})\s*(?:Episode|Ep|E)\s*(\d{1,3})\b/gi, (_, s, e) => `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}`)
    .replace(/\bSeason\s*(\d{1,2})\b/gi, (_, s) => `S${String(s).padStart(2, "0")}`)
    .replace(/\bS\s*(\d{1,2})\s*E\s*(\d{1,3})\b/gi, (_, s, e) => `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}`)
    .replace(/\bS\s*(\d{1,2})\b/gi, (_, s) => `S${String(s).padStart(2, "0")}`)
    .replace(/\s+/g, " ")
    .trim();
}

function queryHasTvToken(q) {
  return /\bS\d{2}(?:E\d{2,3})?\b/i.test(String(q || ""));
}

function buildSearchQuery(params) {
  let q = params.get("q") || params.get("search") || "";
  if (!q) q = config.warmQuery || "warm";
  q = normalizeSearchQueryText(q);
  const tvToken = buildTvToken(params);
  if (tvToken && !queryHasTvToken(q)) q = normalizeSearchQueryText(`${q} ${tvToken}`);
  q = normalizeSearchQueryText(q);
  if (!q) q = config.warmQuery || "warm";

  log("[query]", "built", JSON.stringify({
    t: params.get("t") || "",
    q,
    season: params.get("season") || "",
    ep: params.get("ep") || params.get("episode") || ""
  }));

  return q;
}

async function fetchBrowse(q) {
  const baseUrl = normalizeBaseUrl(config.baseUrl);
  const withAdult = config.withAdult ? "1" : "0";
  const searchPath = `/browse/?q=${encodeURIComponent(q).replace(/%20/g, "+")}&with_adult=${withAdult}`;
  const targetUrl = baseUrl + searchPath;
  const cacheKey = searchPath;
  const cached = getCache(searchCache, cacheKey);

  if (cached) {
    log("[cache] search hit", cacheKey);
    return cached;
  }

  return await withFsLock(async () => {
    const cachedAgain = getCache(searchCache, cacheKey);
    if (cachedAgain) {
      log("[cache] search hit after wait", cacheKey);
      return cachedAgain;
    }

    log("[flaresolverr] browse fetch", targetUrl);
    const r = await fetchHtmlViaFlaresolverr(targetUrl);
    const html = r.html;

    setLimitedCache(searchCache, cacheKey, html,
      Number(config.maxSearchCacheItems || 100),
      Number(config.searchTtlSeconds || 180) * 1000);

    log("[cache] search stored", cacheKey);
    return html;
  });
}

function getRows(html) {
  const m = String(html || "").match(/<table[^>]*class=["'][^"']*search-table[^"']*["'][^>]*>[\s\S]*?<\/table>/i);
  const table = m ? m[0] : html;
  const rows = [];
  const re = /<tr\b[^>]*>[\s\S]*?<\/tr>/gi;
  let row;
  while ((row = re.exec(table))) rows.push(row[0]);
  return rows;
}

function getTds(row) {
  const out = [];
  const re = /<td\b[^>]*>[\s\S]*?<\/td>/gi;
  let m;
  while ((m = re.exec(row))) out.push(m[0]);
  return out;
}

function normalizeTitle(s) {
  return stripTags(s).replace(/_/g, " ").replace(/\./g, " ").replace(/\s+/g, " ").trim();
}

function parseSizeToBytes(s) {
  const raw = String(s || "").replace(/,/g, ".").trim();
  const m = raw.match(/([\d.]+)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)/i);
  if (!m) return 0;
  const n = Number(m[1]);
  const unit = m[2].toUpperCase();
  const mult =
    unit === "B" ? 1 :
    unit === "KB" || unit === "KIB" ? 1024 :
    unit === "MB" || unit === "MIB" ? 1024 ** 2 :
    unit === "GB" || unit === "GIB" ? 1024 ** 3 :
    unit === "TB" || unit === "TIB" ? 1024 ** 4 : 1;
  return Math.max(0, Math.floor(n * mult));
}

function parseDateToRfc822(s) {
  const text = stripTags(s);
  const d = new Date(text);
  if (!isNaN(d.getTime())) return d.toUTCString();
  return new Date().toUTCString();
}

function categoryFromRow(row) {
  if (/href=["']\/movies\//i.test(row)) return 2000;
  if (/href=["']\/anime\//i.test(row)) return 5070;
  if (/href=["']\/tv\//i.test(row)) return 5000;
  return 7000;
}

function attr(tag, name) {
  const re = new RegExp(name + "\\s*=\\s*([\"'])(.*?)\\1", "i");
  const m = String(tag || "").match(re);
  return m ? decodeEntities(m[2]) : "";
}

function findTorrentAnchor(row) {
  const re = /<a\b[^>]*>[\s\S]*?<\/a>/gi;
  let m;
  while ((m = re.exec(row))) {
    const a = m[0];
    const cls = attr(a, "class");
    const href = attr(a, "href");
    if (cls && cls.split(/\s+/).includes("torrent-title-link") && href) {
      const text = a.replace(/<a\b[^>]*>/i, "").replace(/<\/a>/i, "");
      return { href, text };
    }
  }
  re.lastIndex = 0;
  while ((m = re.exec(row))) {
    const a = m[0];
    const href = attr(a, "href");
    if (href && /^\/.+-\d+\/?$/.test(href)) {
      const text = a.replace(/<a\b[^>]*>/i, "").replace(/<\/a>/i, "");
      return { href, text };
    }
  }
  return null;
}

function parseResults(html, req) {
  const rows = getRows(html);
  const items = [];

  for (const row of rows) {
    const found = findTorrentAnchor(row);
    if (!found) continue;

    let relPath = found.href;
    const title = normalizeTitle(found.text);
    if (!title || !relPath) continue;

    if (relPath.startsWith(config.baseUrl)) {
      relPath = relPath.slice(normalizeBaseUrl(config.baseUrl).length);
    }
    if (!relPath.startsWith("/")) continue;
    if (!relPath.endsWith("/")) relPath += "/";

    const id = relPath.replace(/\/$/, "").split("-").pop();
    if (!id || !/^\d+$/.test(id)) continue;

    const tds = getTds(row);
    const sizeText = tds[1] ? stripTags(tds[1]) : "";
    const dateText = tds[3] ? stripTags(tds[3]) : "";
    const seedersText = tds[4] ? stripTags(tds[4]) : "0";
    const leechersText = tds[5] ? stripTags(tds[5]) : "0";

    const size = parseSizeToBytes(sizeText);
    const seeders = Number((seedersText.match(/\d+/) || ["0"])[0]);
    const leechers = Number((leechersText.match(/\d+/) || ["0"])[0]);
    const category = categoryFromRow(row);

    const publicBase = getPublicBaseUrl(req);
    const downloadUrl = `${publicBase}/download?id=${encodeURIComponent(id)}&path=${encodeURIComponent(relPath)}`;
    const commentsUrl = normalizeBaseUrl(config.baseUrl) + relPath;

    items.push({
      id, title, relPath, downloadUrl, commentsUrl, category, size, seeders, leechers,
      pubDate: parseDateToRfc822(dateText)
    });
  }

  log("[parser] rows=", rows.length, "items=", items.length);
  return items;
}

function getPublicBaseUrl(req) {
  if (config.publicUrl) return normalizeBaseUrl(config.publicUrl);
  const host = req.headers.host || `localhost:${PORT}`;
  return `http://${host}`;
}

function torznabCapsXml() {
  return `<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server title="EXT Torznab Proxy" version="1.0.0" />
  <limits max="100" default="50" />
  <searching>
    <search available="yes" supportedParams="q" />
    <movie-search available="yes" supportedParams="q" />
    <tv-search available="yes" supportedParams="q,season,ep" />
  </searching>
  <categories>
    <category id="2000" name="Movies" />
    <category id="5000" name="TV" />
    <category id="5070" name="Anime" />
  </categories>
</caps>`;
}

function torznabErrorXml(code, description) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<error code="${xmlEscape(code)}" description="${xmlEscape(description)}" />`;
}

function torznabResultsXml(items) {
  const itemXml = items.map(item => {
    const peers = item.seeders + item.leechers;
    return `    <item>
      <title>${xmlEscape(item.title)}</title>
      <guid isPermaLink="false">extto-${xmlEscape(item.id)}</guid>
      <link>${xmlEscape(item.downloadUrl)}</link>
      <comments>${xmlEscape(item.commentsUrl)}</comments>
      <pubDate>${xmlEscape(item.pubDate)}</pubDate>
      <category>${xmlEscape(item.category)}</category>
      <size>${xmlEscape(item.size)}</size>
      <enclosure url="${xmlEscape(item.downloadUrl)}" length="${xmlEscape(item.size)}" type="application/x-bittorrent" />
      <torznab:attr name="category" value="${xmlEscape(item.category)}" />
      <torznab:attr name="seeders" value="${xmlEscape(item.seeders)}" />
      <torznab:attr name="peers" value="${xmlEscape(peers)}" />
      <torznab:attr name="grabs" value="0" />
    </item>`;
  }).join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <title>EXT Torznab Proxy</title>
    <description>EXT results via FlareSolverr Torznab proxy</description>
    <link>${xmlEscape(normalizeBaseUrl(config.baseUrl))}</link>
${itemXml}
  </channel>
</rss>`;
}

function apiKeyOk(params) {
  if (!config.apiKey) return true;
  return params.get("apikey") === config.apiKey;
}

async function handleApi(req, res, u) {
  const t = (u.searchParams.get("t") || "search").toLowerCase();

  if (t === "caps") return send(res, 200, "application/xml; charset=utf-8", torznabCapsXml());

  if (!apiKeyOk(u.searchParams)) {
    return send(res, 403, "application/xml; charset=utf-8", torznabErrorXml(100, "Invalid API key"));
  }

  if (t === "search" || t === "movie" || t === "tvsearch") {
    const q = buildSearchQuery(u.searchParams);
    const html = await fetchBrowse(q);
    const items = parseResults(html, req);
    return send(res, 200, "application/rss+xml; charset=utf-8", torznabResultsXml(items));
  }

  return send(res, 400, "application/xml; charset=utf-8", torznabErrorXml(200, "Unsupported function"));
}

function parseMagnetJson(text) {
  const body = pick(/<pre>([\s\S]*?)<\/pre>/, text) || text;
  return JSON.parse(body);
}

// ─── PATCHED: magnet POST ide kroz FlareSolverr (zaobilazi Cloudflare) ───
async function postMagnetDirect({ id, ts, hmac, csrf, referer, cookies, userAgent }) {
  const postData = new URLSearchParams({
    torrent_id: id,
    action: "get_magnet",
    timestamp: ts,
    hmac,
    sessid: csrf
  }).toString();

  const fsRes = await fsCall({
    cmd: "request.post",
    url: normalizeBaseUrl(config.baseUrl) + "/ajax/getTorrentMagnet.php",
    session: config.sessionName,
    postData: postData,
    maxTimeout: Number(config.requestTimeoutMs || 120000)
  });

  const solution = fsRes && fsRes.solution ? fsRes.solution : {};
  const text = solution.response || "";

  if (!text) throw new Error("magnet ajax empty response");

  let json;
  try {
    json = parseMagnetJson(text);
  } catch {
    throw new Error("magnet ajax bad JSON: " + text.slice(0, 300));
  }

  return json;
}
// ─── END PATCH ───

async function fetchMagnetOnce(relPath, id) {
  await ensureSession();
  const url = normalizeBaseUrl(config.baseUrl) + relPath;
  lastUsed = Date.now();

  const pageRes = await fsCall({
    cmd: "request.get",
    url,
    session: config.sessionName,
    maxTimeout: Number(config.requestTimeoutMs || 120000)
  });

  const solution = pageRes && pageRes.solution ? pageRes.solution : {};
  const page = solution.response || "";

  const pageToken = pick(/window\.pageToken\s*=\s*'([^']+)'/, page);
  const csrf = pick(/window\.csrfToken\s*=\s*'([^']+)'/, page);
  if (!pageToken || !csrf) throw new Error("token parse failed");

  const ts = Math.floor(Date.now() / 1000).toString();
  const hmac = crypto.createHash("sha256").update(`${id}|${ts}|${pageToken}`).digest("hex");

  const json = await postMagnetDirect({
    id, ts, hmac, csrf,
    referer: url,
    cookies: solution.cookies,
    userAgent: getUserAgent(solution)
  });

  if (!json.success || !json.magnet) throw new Error(json.error || "magnet failed");

  return json.magnet;
}

async function fetchMagnet(relPath, id) {
  try {
    return await fetchMagnetOnce(relPath, id);
  } catch (e) {
    warn("[retry] magnet failed, resetting session:", e.message || e);
    await resetSession();
    return await fetchMagnetOnce(relPath, id);
  }
}

// ─── PATCHED: handleDownload returns magnet as body for torrent clients ───
async function handleDownload(req, res, u) {
  const id = u.searchParams.get("id") || "";
  const relPath = u.searchParams.get("path") || "";

  if (!id || !relPath || !relPath.startsWith("/") || !relPath.endsWith("/")) {
    return send(res, 400, "text/plain; charset=utf-8", "bad download parameters");
  }

  const accept = String(req.headers.accept || "").toLowerCase();
  const isBrowser = accept.includes("text/html") && !accept.includes("application/x-bittorrent");

  const cached = getCache(magnetCache, id);

  async function getMagnetString() {
    if (cached) {
      log("[cache] magnet hit", id);
      return sanitizeMagnet(cached);
    }

    return await withFsLock(async () => {
      const cachedAgain = getCache(magnetCache, id);
      if (cachedAgain) {
        log("[cache] magnet hit after wait", id);
        return sanitizeMagnet(cachedAgain);
      }

      log("[flaresolverr] magnet fetch", id);
      const m = await fetchMagnet(relPath, id);

      setLimitedCache(magnetCache, id, m,
        Number(config.maxMagnetCacheItems || 1000),
        Number(config.magnetTtlMinutes || 1440) * 60 * 1000);

      log("[cache] magnet stored", id);
      return sanitizeMagnet(m);
    });
  }

  try {
    const safeMagnet = await getMagnetString();

    if (!safeMagnet || !safeMagnet.startsWith("magnet:")) {
      return send(res, 500, "text/plain; charset=utf-8", "invalid magnet");
    }

    log("[download] id=", id, "accept=", accept, "browser=", isBrowser);

    if (isBrowser) {
      // Browser: 302 redirect na magnet
      res.writeHead(302, {
        Location: safeMagnet,
        "cache-control": "private, max-age=86400"
      });
      return res.end();
    }

    // Torrent klijent (Jackett, Elementum, Sonarr, Radarr): 200 s magnetom u tijelu
    const body = safeMagnet + "\n";
    res.writeHead(200, {
      "content-type": "application/x-bittorrent",
      "content-length": Buffer.byteLength(body),
      "cache-control": "private, max-age=86400"
    });
    return res.end(body);
  } catch (e) {
    error("[download] failed for id=", id, e.message || e);
    return send(res, 500, "text/plain; charset=utf-8", String(e.message || e));
  }
}
// ─── END PATCH ───

async function doWarm() {
  const start = Date.now();
  const q = config.warmQuery || "warm";
  currentWarm = { running: true, query: q, startedAt: new Date().toISOString() };

  try {
    const withAdult = config.withAdult ? "1" : "0";
    const warmCacheKey = `/browse/?q=${encodeURIComponent(q).replace(/%20/g, "+")}&with_adult=${withAdult}`;
    searchCache.delete(warmCacheKey);
    await fetchBrowse(q);

    const durationMs = Date.now() - start;
    const duration = Math.round(durationMs / 1000);

    lastWarm = {
      ok: true, query: q, durationSeconds: duration, durationMs,
      cacheBypassed: true,
      state: duration >= Number(config.coldThresholdSeconds || 30) ? "COLD_OR_CHALLENGE" : "WARM",
      at: new Date().toISOString()
    };
    log("[warm]", `query=${q}`, `duration=${duration}s`, `durationMs=${durationMs}`, `state=${lastWarm.state}`, "cacheBypassed=true");
    return lastWarm;
  } catch (e) {
    const durationMs = Date.now() - start;
    const duration = Math.round(durationMs / 1000);
    lastWarm = {
      ok: false, query: q, durationSeconds: duration, durationMs,
      cacheBypassed: true, state: "ERROR", error: e.message || String(e),
      at: new Date().toISOString()
    };
    warn("[warm] failed", e.message || e);
    return lastWarm;
  } finally {
    currentWarm = null;
  }
}

function startWarmTimer() {
  async function tick(reason) {
    if (!config.warmEnabled) return;
    log("[warm] scheduled", reason || "timer");
    try { await doWarm(); } catch (e) { warn("[warm] background failed", e.message || e); }
  }
  setTimeout(() => { tick("startup").catch(e => warn("[warm] startup failed", e.message || e)); }, 5000);
  setInterval(() => { tick("interval").catch(e => warn("[warm] interval failed", e.message || e)); },
    Math.max(1, Number(config.warmIntervalMinutes || 10)) * 60 * 1000);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", chunk => {
      data += chunk;
      if (data.length > 1024 * 1024) reject(new Error("body too large"));
    });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function boolFromForm(v) {
  return v === "on" || v === "true" || v === "1";
}

function formatDurationView(seconds, ms) {
  if (typeof ms === "number" && ms > 0 && seconds < 1) return `${ms}ms`;
  return `${seconds || 0}s`;
}

function getCurrentWarmSeconds() {
  if (!currentWarm || !currentWarm.startedAt) return null;
  return Math.max(0, Math.floor((Date.now() - new Date(currentWarm.startedAt).getTime()) / 1000));
}

function getCurrentWarmState() {
  const rs = getCurrentWarmSeconds();
  if (rs === null) return null;
  return rs >= Number(config.coldThresholdSeconds || 30) ? "CHALLENGE" : "RUNNING";
}

function getWarmBadgeText() {
  if (!config.warmEnabled) return "Off";
  const rs = getCurrentWarmSeconds();
  const st = getCurrentWarmState();
  if (rs !== null) return st === "CHALLENGE" ? `Challenge ${rs}s` : `Running ${rs}s`;
  return "On";
}

function getWarmAgeView() {
  const rs = getCurrentWarmSeconds();
  const st = getCurrentWarmState();
  if (rs !== null) return st === "CHALLENGE" ? `${rs}s CHALLENGE` : `${rs}s RUNNING`;
  if (!lastWarm || !lastWarm.ok || !lastWarm.at) return "—";
  const age = Math.max(0, Math.floor((Date.now() - new Date(lastWarm.at).getTime()) / 1000));
  return `${age}s WARM`;
}

function getLastWarmRunView() {
  const rs = getCurrentWarmSeconds();
  const st = getCurrentWarmState();
  if (rs !== null) return st === "CHALLENGE" ? `${rs}s CHALLENGE` : `${rs}s RUNNING`;
  if (!lastWarm) return "—";
  if (!lastWarm.ok) return `${formatDurationView(lastWarm.durationSeconds || 0, lastWarm.durationMs)} ERROR`;
  return `${formatDurationView(lastWarm.durationSeconds || 0, lastWarm.durationMs)} ${lastWarm.state || "UNKNOWN"}`;
}

function settingsHtml(message = "") {
  const c = config;
  const publicUrlValue = c.publicUrl || "";
  const apiKey = c.apiKey || "";
  const currentWarmSeconds = getCurrentWarmSeconds();
  const currentWarmState = getCurrentWarmState();
  const lastWarmAgeSeconds = lastWarm && lastWarm.ok && lastWarm.at
    ? Math.max(0, Math.floor((Date.now() - new Date(lastWarm.at).getTime()) / 1000))
    : null;

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EXT Torznab Proxy</title>
<style>
body{margin:0;font-family:system-ui,sans-serif;background:#0f172a;color:#e5e7eb;padding:20px}
.page{max-width:1100px;margin:0 auto}
h1{margin:0 0 10px}
.card{background:#1f2937;border-radius:12px;padding:20px;margin-bottom:16px}
code{background:#030712;color:#dbeafe;padding:5px 8px;border-radius:6px;font-size:12px}
input,select{width:100%;min-height:38px;background:#0b1220;color:#e5e7eb;border:1px solid #4b5563;border-radius:8px;padding:8px 10px;font:inherit}
label{display:block;margin-bottom:5px;font-weight:600;font-size:13px}
.field{margin-bottom:12px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
button{background:#3b82f6;color:white;border:0;border-radius:8px;padding:10px 16px;font-weight:700;cursor:pointer}
.btn-sec{background:#374151}
.badge{display:inline-block;padding:6px 10px;background:#111827;border-radius:20px;font-size:12px;margin-right:6px}
</style>
</head>
<body>
<div class="page">
<h1>EXT Torznab Proxy</h1>
<div class="card">
<span class="badge">Session: ${sessionReady ? "Ready" : "Cold"}</span>
<span class="badge">Warm: ${htmlEscape(getWarmBadgeText())}</span>
<span class="badge">FlareSolverr: ${fsBusy ? "Busy" : "Idle"}</span>
</div>
<div class="card">
<h3>Prowlarr Generic Torznab</h3>
<p>URL: <code>${htmlEscape(publicUrlValue || "http://" + "localhost" + ":8998")}</code></p>
<p>API Path: <code>/api</code></p>
<p>API Key: <code>${htmlEscape(apiKey)}</code></p>
</div>
<form method="post" action="/settings">
<div class="card">
<h3>Site and FlareSolverr</h3>
<div class="grid">
<div class="field"><label>Base URL</label>
<select name="baseUrl">
<option value="https://extranet.torrentbay.st" ${normalizeBaseUrl(c.baseUrl) === "https://extranet.torrentbay.st" ? "selected" : ""}>https://extranet.torrentbay.st</option>
<option value="https://ext.to" ${normalizeBaseUrl(c.baseUrl) === "https://ext.to" ? "selected" : ""}>https://ext.to</option>
</select></div>
<div class="field"><label>Public URL</label><input type="text" name="publicUrl" value="${htmlEscape(c.publicUrl)}"></div>
<div class="field"><label>FlareSolverr URL</label><input type="text" name="flaresolverrUrl" value="${htmlEscape(c.flaresolverrUrl)}"></div>
<div class="field"><label>Session Name</label><input type="text" name="sessionName" value="${htmlEscape(c.sessionName)}"></div>
<div class="field"><label>API Key</label><input type="text" name="apiKey" value="${htmlEscape(c.apiKey)}"></div>
<div class="field"><label>Request Timeout Ms</label><input type="number" name="requestTimeoutMs" value="${htmlEscape(c.requestTimeoutMs)}"></div>
</div>
</div>
<div class="card">
<h3>Warm and Cache</h3>
<div class="grid">
<div class="field"><label>Warm Query</label><input type="text" name="warmQuery" value="${htmlEscape(c.warmQuery)}"></div>
<div class="field"><label>Warm Interval Minutes</label><input type="number" name="warmIntervalMinutes" value="${htmlEscape(c.warmIntervalMinutes)}"></div>
<div class="field"><label>Cold Threshold Seconds</label><input type="number" name="coldThresholdSeconds" value="${htmlEscape(c.coldThresholdSeconds)}"></div>
<div class="field"><label>Search TTL Seconds</label><input type="number" name="searchTtlSeconds" value="${htmlEscape(c.searchTtlSeconds)}"></div>
<div class="field"><label>Magnet TTL Minutes</label><input type="number" name="magnetTtlMinutes" value="${htmlEscape(c.magnetTtlMinutes)}"></div>
<div class="field"><label>Idle Timeout Minutes</label><input type="number" name="idleTimeoutMinutes" value="${htmlEscape(c.idleTimeoutMinutes)}"></div>
<div class="field"><label>Max Search Cache Items</label><input type="number" name="maxSearchCacheItems" value="${htmlEscape(c.maxSearchCacheItems)}"></div>
<div class="field"><label>Max Magnet Cache Items</label><input type="number" name="maxMagnetCacheItems" value="${htmlEscape(c.maxMagnetCacheItems)}"></div>
</div>
<div class="field"><label><input type="checkbox" name="warmEnabled" ${c.warmEnabled ? "checked" : ""} style="width:auto;min-height:auto"> Enable internal warm timer</label></div>
</div>
<button type="submit">Save Settings</button>
<button type="button" class="btn-sec" onclick="fetch('/warm').then(()=>location.reload())">Manual Warm</button>
<button type="button" class="btn-sec" onclick="fetch('/cache/clear').then(()=>location.reload())">Clear Cache</button>
<button type="button" class="btn-sec" onclick="fetch('/session/reset').then(()=>location.reload())">Session Reset</button>
</form>
</div>
</body>
</html>`;
}

function parseForm(body) {
  const p = new URLSearchParams(body);
  return {
    baseUrl: normalizeBaseUrl(p.get("baseUrl")),
    publicUrl: normalizeBaseUrl(p.get("publicUrl")),
    flaresolverrUrl: p.get("flaresolverrUrl"),
    sessionName: p.get("sessionName"),
    apiKey: p.get("apiKey"),
    withAdult: p.has("withAdult") ? boolFromForm(p.get("withAdult")) : config.withAdult,
    stripSeasonFromSearch: p.has("stripSeasonFromSearch") ? boolFromForm(p.get("stripSeasonFromSearch")) : config.stripSeasonFromSearch,
    warmEnabled: p.has("warmEnabled") ? boolFromForm(p.get("warmEnabled")) : config.warmEnabled,
    warmQuery: p.get("warmQuery"),
    warmIntervalMinutes: Number(p.get("warmIntervalMinutes") || 10),
    coldThresholdSeconds: Number(p.get("coldThresholdSeconds") || 30),
    searchTtlSeconds: Number(p.get("searchTtlSeconds") || 180),
    magnetTtlMinutes: Number(p.get("magnetTtlMinutes") || 1440),
    maxSearchCacheItems: Number(p.get("maxSearchCacheItems") || 100),
    maxMagnetCacheItems: Number(p.get("maxMagnetCacheItems") || 1000),
    requestTimeoutMs: Number(p.get("requestTimeoutMs") || 120000),
    idleTimeoutMinutes: Number(p.get("idleTimeoutMinutes") || 360)
  };
}

function send(res, status, type, body) {
  res.writeHead(status, {
    "content-type": type,
    "cache-control": "no-store"
  });
  res.end(body);
}

setInterval(() => {
  const r = clearExpiredCache();
  if (r.removedSearches > 0 || r.removedMagnets > 0) {
    log("[cache] expired removed searches=", r.removedSearches, "magnets=", r.removedMagnets);
  }
}, 5 * 60 * 1000);

setInterval(async () => {
  const idle = Date.now() - lastUsed;
  const idleTimeoutMs = Number(config.idleTimeoutMinutes || 360) * 60 * 1000;
  if (sessionReady && idle > idleTimeoutMs) {
    log("[idle] destroying session after", config.idleTimeoutMinutes, "minutes");
    await destroySessionUnlocked();
  }
}, 60 * 1000);

http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, "http://x");

    if (req.method === "GET" && u.pathname === "/") {
      const savedMessage = u.searchParams.get("saved") === "1" ? "Settings saved." : "";
      return send(res, 200, "text/html; charset=utf-8", settingsHtml(savedMessage));
    }

    if (req.method === "POST" && u.pathname === "/settings") {
      const body = await readBody(req);
      saveConfig(parseForm(body));
      res.writeHead(303, { Location: "/?saved=1" });
      return res.end();
    }

    if (req.method === "GET" && u.pathname === "/health") {
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true, app: "extto-torznab-proxy", version: "1.0.0",
        baseUrl: config.baseUrl, flaresolverrUrl: config.flaresolverrUrl,
        sessionName: config.sessionName, sessionReady, sessionBusy, fsBusy,
        idleSeconds: Math.floor((Date.now() - lastUsed) / 1000),
        searchCacheItems: searchCache.size, magnetCacheItems: magnetCache.size,
        warmEnabled: config.warmEnabled, warmIntervalMinutes: config.warmIntervalMinutes,
        coldThresholdSeconds: config.coldThresholdSeconds,
        warmRunning: !!currentWarm,
        currentWarm: currentWarm ? {
          query: currentWarm.query, startedAt: currentWarm.startedAt,
          runningSeconds: Math.max(0, Math.floor((Date.now() - new Date(currentWarm.startedAt).getTime()) / 1000))
        } : null,
        warmAgeSeconds: lastWarm && lastWarm.ok ? Math.max(0, Math.floor((Date.now() - new Date(lastWarm.at).getTime()) / 1000)) : null,
        lastWarmRun: lastWarm ? {
          durationSeconds: lastWarm.durationSeconds, durationMs: lastWarm.durationMs,
          state: lastWarm.state, at: lastWarm.at
        } : null,
        lastWarm
      }, null, 2));
    }

    if (req.method === "GET" && u.pathname === "/warm") {
      const result = await doWarm();
      return send(res, result.ok ? 200 : 500, "application/json; charset=utf-8", JSON.stringify(result, null, 2));
    }

    if (req.method === "POST" && u.pathname === "/api-key/regenerate") {
      const nextApiKey = randomApiKey();
      saveConfig({ ...config, apiKey: nextApiKey });
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, apiKey: nextApiKey }, null, 2));
    }

    if (req.method === "GET" && u.pathname === "/cache/clear") {
      const s = searchCache.size;
      const m = magnetCache.size;
      searchCache.clear();
      magnetCache.clear();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, clearedSearches: s, clearedMagnets: m }, null, 2));
    }

    if (req.method === "GET" && u.pathname === "/session/reset") {
      await destroySessionUnlocked();
      sessionReady = false;
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, sessionReset: true }, null, 2));
    }

    if (req.method === "GET" && u.pathname === "/api") {
      return await handleApi(req, res, u);
    }

    if (req.method === "GET" && u.pathname === "/download") {
      return await handleDownload(req, res, u);
    }

    return send(res, 404, "text/plain; charset=utf-8", "not found");
  } catch (e) {
    error("[error]", e && e.stack ? e.stack : e);
    return send(res, 500, "text/plain; charset=utf-8", String(e.message || e));
  }
}).listen(PORT, "0.0.0.0", () => {
  log("extto-torznab-proxy listening on :" + PORT);
  log("config:", CONFIG_FILE);
  log("base:", config.baseUrl);
  log("flaresolverr:", config.flaresolverrUrl);
  log("session:", config.sessionName);
  startWarmTimer();
});
