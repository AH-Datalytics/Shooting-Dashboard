/**
 * Shooting Dashboard Proxy Server
 * Fetches URLs server-side to bypass CORS restrictions.
 *
 * Endpoints:
 *   GET /proxy?url=https://...        — fetch any URL, return body with CORS headers
 *   GET /durham                       — scrape Durham archive, return latest PDF URL + ADID
 *   GET /health                       — health check
 *
 * Usage:
 *   npm install
 *   node server.js
 *   (or: PORT=3001 node server.js)
 */

const http  = require('http');
const https = require('https');
const url   = require('url');

const PORT = process.env.PORT || 3000;

// ─── Allowed origins (set to '*' to allow all, or restrict to your domain) ───
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

// ─── Allowlist of domains the proxy will fetch (add more as needed) ──────────
const ALLOWED_DOMAINS = [
  'www.durhamnc.gov',
  'durhamnc.gov',
  // Add others here if needed, e.g.:
  // 'www.nashville.gov',
  // 'city.milwaukee.gov',
];

// Set to false to disable domain allowlist and allow any domain
const ENFORCE_ALLOWLIST = process.env.ENFORCE_ALLOWLIST !== 'false';

// ─── CORS headers ─────────────────────────────────────────────────────────────
function corsHeaders() {
  return {
    'Access-Control-Allow-Origin':  ALLOWED_ORIGIN,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

// ─── Fetch a URL server-side (follows redirects) ─────────────────────────────
function fetchUrl(targetUrl, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const parsed = url.parse(targetUrl);
    const lib = parsed.protocol === 'https:' ? https : http;

    const options = {
      hostname: parsed.hostname,
      port:     parsed.port,
      path:     parsed.path,
      method:   'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; ShootingDashboardProxy/1.0)',
        'Accept':     '*/*',
      },
      timeout: timeoutMs,
    };

    const req = lib.request(options, (res) => {
      // Follow redirects (up to 5)
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : parsed.protocol + '//' + parsed.hostname + res.headers.location;
        return fetchUrl(redirectUrl, timeoutMs).then(resolve).catch(reject);
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve({
        status:      res.statusCode,
        contentType: res.headers['content-type'] || 'application/octet-stream',
        body:        Buffer.concat(chunks),
      }));
      res.on('error', reject);
    });

    req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    req.on('error', reject);
    req.end();
  });
}

// ─── Durham: scrape archive page, return latest ADID + PDF URL ────────────────
async function getDurhamLatest() {
  const archiveUrl = 'https://www.durhamnc.gov/Archive.aspx?AMID=211';
  const result = await fetchUrl(archiveUrl);

  if (result.status !== 200) {
    throw new Error(`Archive page returned HTTP ${result.status}`);
  }

  const html = result.body.toString('utf8');

  // Find all ADID values from links like Archive.aspx?ADID=XXXX
  const matches = [...html.matchAll(/ADID=(\d+)/g)].map(m => parseInt(m[1]));
  if (!matches.length) {
    throw new Error('No ADID links found in archive page HTML');
  }

  const latestAdid = Math.max(...matches);
  const pdfUrl = `https://www.durhamnc.gov/ArchiveCenter/ViewFile/Item/${latestAdid}`;

  return { adid: latestAdid, pdfUrl };
}

// ─── Request handler ──────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const parsed   = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // Handle preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders());
    res.end();
    return;
  }

  // Health check
  if (pathname === '/health') {
    res.writeHead(200, { ...corsHeaders(), 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, ts: new Date().toISOString() }));
    return;
  }

  // Durham latest PDF endpoint
  if (pathname === '/durham') {
    try {
      const result = await getDurhamLatest();
      res.writeHead(200, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // General proxy endpoint: /proxy?url=https://...
  if (pathname === '/proxy') {
    const targetUrl = parsed.query.url;

    if (!targetUrl) {
      res.writeHead(400, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing ?url= parameter' }));
      return;
    }

    // Validate URL
    let parsedTarget;
    try {
      parsedTarget = url.parse(targetUrl);
      if (!['http:', 'https:'].includes(parsedTarget.protocol)) throw new Error('Bad protocol');
    } catch (e) {
      res.writeHead(400, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid URL' }));
      return;
    }

    // Allowlist check
    if (ENFORCE_ALLOWLIST && !ALLOWED_DOMAINS.includes(parsedTarget.hostname)) {
      res.writeHead(403, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: `Domain not allowed: ${parsedTarget.hostname}. Add it to ALLOWED_DOMAINS in server.js.`
      }));
      return;
    }

    try {
      const result = await fetchUrl(targetUrl);
      res.writeHead(result.status, {
        ...corsHeaders(),
        'Content-Type': result.contentType,
      });
      res.end(result.body);
    } catch (err) {
      res.writeHead(502, { ...corsHeaders(), 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Upstream fetch failed: ' + err.message }));
    }
    return;
  }

  // 404
  res.writeHead(404, { ...corsHeaders(), 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found', endpoints: ['/health', '/durham', '/proxy?url=...'] }));
});

server.listen(PORT, () => {
  console.log(`Shooting Dashboard Proxy running on http://localhost:${PORT}`);
  console.log(`  /health          — health check`);
  console.log(`  /durham          — Durham latest PDF ADID`);
  console.log(`  /proxy?url=...   — general CORS proxy`);
  console.log(`  Allowlist: ${ENFORCE_ALLOWLIST ? ALLOWED_DOMAINS.join(', ') : 'disabled (all domains)'}`);
});
