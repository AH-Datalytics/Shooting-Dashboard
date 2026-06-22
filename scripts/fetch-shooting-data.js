/**

 * fetch-shooting-data.js

 * Runs via GitHub Actions to fetch blocked city data server-side

 * and write results to data/manual-auto.json

 */



const https = require('https');

const http  = require('http');

const fs    = require('fs');

const path  = require('path');



// ─── Helpers ──────────────────────────────────────────────────────────────────



function fetchUrl(targetUrl, timeoutMs = 20000, _maxRedirects = 10) {

  return new Promise((resolve, reject) => {

    const parsed = new URL(targetUrl);

    const lib = parsed.protocol === 'https:' ? https : http;

    const options = {

      hostname: parsed.hostname,

      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),

      path: parsed.pathname + parsed.search,

      method: 'GET',

      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36' },

      timeout: timeoutMs,

    };

    const req = lib.request(options, (res) => {

      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {

        if (_maxRedirects <= 0) return reject(new Error('Too many redirects'));

        const redirect = res.headers.location.startsWith('http')

          ? res.headers.location

          : parsed.origin + res.headers.location;

        return fetchUrl(redirect, timeoutMs, _maxRedirects - 1).then(resolve).catch(reject);

      }

      const chunks = [];

      res.on('data', c => chunks.push(c));

      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));

      res.on('error', reject);

    });

    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });

    req.on('error', reject);

    req.end();

  });

}



// ─── Claude Vision API helper ─────────────────────────────────────────────────

async function callClaudeVision(base64Data, mediaType, prompt, { model = 'claude-haiku-4-5-20251001', maxTokens = 256 } = {}) {
  const isDocument = mediaType === 'application/pdf';
  const content = [
    isDocument
      ? { type: 'document', source: { type: 'base64', media_type: mediaType, data: base64Data } }
      : { type: 'image', source: { type: 'base64', media_type: mediaType, data: base64Data } },
    { type: 'text', text: prompt }
  ];
  const body = JSON.stringify({ model, max_tokens: maxTokens, messages: [{ role: 'user', content }] });
  const data = await new Promise((resolve, reject) => {
    const req = require('https').request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body)
      }
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch(e) { reject(e); } });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
  // Error handling: check for API errors, rate limits, etc.
  if (data.type === 'error' || data.error) {
    const msg = data.error?.message || JSON.stringify(data.error || data);
    throw new Error('Claude API error: ' + msg);
  }
  return (data.content?.[0]?.text || '').trim();
}


// ─── Power BI Query API helper ───────────────────────────────────────────────

const zlib = require('zlib');

function pbiQuery(cluster, reportKey, modelId, datasetId, query) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      version: '1.0.0',
      queries: [{
        Query: { Commands: [{ SemanticQueryDataShapeCommand: { Query: query } }] },
        QueryId: '',
        ApplicationContext: { DatasetId: datasetId, Sources: [] }
      }],
      cancelQueries: [],
      modelId
    });

    const req = https.request({
      hostname: cluster,
      path: '/public/reports/querydata?synchronous=true',
      method: 'POST',
      headers: {
        'X-PowerBI-ResourceKey': reportKey,
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip'
      }
    }, res => {
      let stream = res;
      if (res.headers['content-encoding'] === 'gzip') stream = res.pipe(zlib.createGunzip());
      const chunks = [];
      stream.on('data', c => chunks.push(c));
      stream.on('end', () => {
        try {
          const j = JSON.parse(Buffer.concat(chunks).toString());
          if (j.results && j.results[0].result.data.dsr) {
            resolve(j.results[0].result.data.dsr);
          } else {
            const errMsg = j.DataShapes?.[0]?.['odata.error']?.message?.value || JSON.stringify(j).substring(0, 200);
            reject(new Error('PBI query error: ' + errMsg));
          }
        } catch (e) {
          reject(new Error('PBI parse error: ' + e.message));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('PBI timeout')); });
    req.write(body);
    req.end();
  });
}

function pbiDataShapeQuery(cluster, reportKey, modelId, datasetId, command, sources = []) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      version: '1.0.0',
      queries: [{
        Query: { Commands: [{ SemanticQueryDataShapeCommand: command }] },
        QueryId: '',
        ApplicationContext: { DatasetId: datasetId, Sources: sources }
      }],
      cancelQueries: [],
      modelId
    });

    const req = https.request({
      hostname: cluster,
      path: '/public/reports/querydata?synchronous=true',
      method: 'POST',
      headers: {
        'X-PowerBI-ResourceKey': reportKey,
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip'
      }
    }, res => {
      let stream = res;
      if (res.headers['content-encoding'] === 'gzip') stream = res.pipe(zlib.createGunzip());
      const chunks = [];
      stream.on('data', c => chunks.push(c));
      stream.on('end', () => {
        try {
          const j = JSON.parse(Buffer.concat(chunks).toString());
          if (j.results && j.results[0].result.data.dsr) {
            resolve(j.results[0].result.data.dsr);
          } else {
            reject(new Error('PBI visual query error: ' + JSON.stringify(j).substring(0, 200)));
          }
        } catch (e) {
          reject(new Error('PBI visual parse error: ' + e.message));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('PBI visual timeout')); });
    req.write(body);
    req.end();
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchUrlRetry(targetUrl, options = {}) {
  const attempts = options.attempts || 3;
  const timeoutMs = options.timeoutMs || 30000;
  const label = options.label || targetUrl;
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const resp = await fetchUrl(targetUrl, timeoutMs);
      if (resp.status >= 200 && resp.status < 300) return resp;
      lastError = new Error(label + ': HTTP ' + resp.status);
    } catch (e) {
      lastError = e;
    }

    if (attempt < attempts) {
      console.log(label + ': attempt ' + attempt + ' failed (' + lastError.message + '), retrying...');
      await sleep(750 * attempt);
    }
  }

  throw lastError || new Error(label + ': request failed');
}

async function fetchJsonRetry(targetUrl, options = {}) {
  const resp = await fetchUrlRetry(targetUrl, options);
  return JSON.parse(resp.body.toString('utf8'));
}

// Extract {year: count} map from PBI DSR response (DM1 rows with C: [year, count])
function parsePbiYearCounts(dsr) {
  const result = {};
  const dm1 = dsr.DS[0].PH[1]?.DM1 || dsr.DS[0].PH[0]?.DM1;
  if (!dm1) return result;
  for (const row of dm1) {
    if (!row.C || row.C.length < 2) continue;
    result[row.C[0]] = row.C[1];
  }
  return result;
}

function pbiFirstScalar(dsr) {
  const row = dsr?.DS?.[0]?.PH?.[0]?.DM0?.[0];
  if (!row) return null;
  for (let i = 0; i < 10; i++) {
    if (row['M' + i] != null) return row['M' + i];
    if (row['G' + i] != null) return row['G' + i];
    if (row['A' + i] != null) return row['A' + i];
  }
  if (Array.isArray(row.C) && row.C.length) return row.C[0];
  return null;
}

function formatPbiDate(value) {
  if (value == null) return null;
  if (typeof value === 'number') {
    if (value > 1000000000000) return new Date(value).toISOString().slice(0, 10);
    return null;
  }
  if (typeof value === 'string') {
    let m = value.match(/(\d{4})-(\d{2})-(\d{2})/);
    if (m) return m[1] + '-' + m[2] + '-' + m[3];
    m = value.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (m) return m[3] + '-' + m[1].padStart(2, '0') + '-' + m[2].padStart(2, '0');
  }
  return null;
}

async function pbiAsOf(cluster, reportKey, modelId, datasetId, query, label) {
  const dsr = await pbiQuery(cluster, reportKey, modelId, datasetId, query);
  const value = pbiFirstScalar(dsr);
  const asof = formatPbiDate(value);
  if (!asof) throw new Error(label + ': no asof date in PBI response');
  return asof;
}


// ─── Power BI wait helper ─────────────────────────────────────────────────────

async function waitForPowerBI(page, fallbackMs = 10000) {
  // Try to wait for Power BI visual containers to render, then short safety wait
  try {
    await page.waitForSelector('.visual-container, .visualContainer, [class*="visual"]', { timeout: 30000 });
    await page.waitForTimeout(Math.min(fallbackMs, 5000));
  } catch {
    // Selector not found — fall back to fixed wait
    await page.waitForTimeout(fallbackMs);
  }
}


// ─── CSV parsing ──────────────────────────────────────────────────────────────

function parseCsvLine(line) {
  const cols = [];
  let cur = '', inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
      else if (ch === '"') inQuote = false;
      else cur += ch;
    } else {
      if (ch === '"') inQuote = true;
      else if (ch === ',') { cols.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
  }
  cols.push(cur.trim());
  return cols;
}


// ─── PDF parsing ──────────────────────────────────────────────────────────────



async function extractPdfTokens(buffer, pageNum = 1) {

  // pdfjs-dist is installed at repo root (node_modules/)

  let pdfjsLib;

  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }

  catch(e) { pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }

  pdfjsLib.GlobalWorkerOptions.workerSrc = false;

  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(buffer) }).promise;

  const page = await pdf.getPage(pageNum);

  const tc = await page.getTextContent();

  const raw = tc.items.map(i => i.str).filter(s => s.length > 0);



  // Collapse runs of single characters caused by custom font encoding

  // e.g. ['N','o','n','-','F','a','t','a','l'] -> ['Non-Fatal']

  const merged = [];

  let run = '';

  for (const tok of raw) {

    if (tok.length === 1 && tok.trim().length > 0) {

      run += tok;

    } else {

      if (run.length > 0) { merged.push(run.trim()); run = ''; }

      const t = tok.trim();

      if (t.length > 0) merged.push(t);

    }

  }

  if (run.length > 0) merged.push(run.trim());



  // Further pass: re-split merged tokens on whitespace in case multiple words merged

  const tokens = [];

  for (const t of merged) {

    const parts = t.split(/\s+/).filter(p => p.length > 0);

    tokens.push(...parts);

  }

  return tokens;

}



// ─── Detroit ──────────────────────────────────────────────────────────────────



async function fetchDetroit() {

  // Try recent dates going backwards to find the latest PDF

  // Two known filename patterns: "YYMMDD DPD Stats.pdf" and "YYMMDD DPD Weekly Stats.pdf"

  const today = new Date();

  let resp = null;

  let pdfUrl = null;

  const patterns = ['DPD%20Stats', 'DPD%20Weekly%20Stats'];

  

  for (let back = 0; back <= 10; back++) {

    const d = new Date(today);

    d.setDate(d.getDate() - back);

    const yyyy = d.getFullYear();

    const mm   = String(d.getMonth()+1).padStart(2,'0');

    const dd   = String(d.getDate()).padStart(2,'0');

    const yy   = String(yyyy).slice(2);

    let found = false;

    for (const pat of patterns) {

      pdfUrl = `https://detroitmi.gov/sites/detroitmi.localhost/files/events/${yyyy}-${mm}/${yy}${mm}${dd}%20${pat}.pdf`;

      console.log('Detroit: trying', pdfUrl);

      resp = await fetchUrl(pdfUrl);

      if (resp.status === 200) { found = true; break; }

      console.log('Detroit:   status=' + resp.status);

    }

    if (found) break;

  }

  

  if (!resp || resp.status !== 200) throw new Error(`Detroit PDF not found (tried 11 dates x 2 patterns)`);



  const tokens = await extractPdfTokens(resp.body);

  const text = tokens.join(' ');



  // Date - try text first, fall back to URL filename (YYMMDD e.g. 260219 = 2026-02-19)

  const dateMatch = text.match(/\w+day,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})/i);

  let asof = null;

  if (dateMatch) {

    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};

    const mo = months[dateMatch[1].toLowerCase()];

    asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;

  }

  if (!asof) {

    const fnMatch = pdfUrl.match(/\/(\d{2})(\d{2})(\d{2})%20DPD/);

    if (fnMatch) asof = `20${fnMatch[1]}-${fnMatch[2]}-${fnMatch[3]}`;

  }



  // Join all tokens and search for Non-Fatal Shooting row

  // Tokens may be partially merged so search the joined string

  const joined = tokens.join(' ');

  const nfsMatch = joined.match(/Non.?Fatal\s*Shooting[\s\S]*?(?=\w+Homicide|\w+Sex|\w+Assault|\w+Robbery|\w+Burglary|$)/i);

  if (!nfsMatch) throw new Error('Non-Fatal Shooting row not found. Tokens: ' + tokens.slice(0,60).join('|'));



  // Extract all numbers from the matched section

  const nums = [];

  const numMatches = nfsMatch[0].matchAll(/-?[\d,]+(?:\.\d+)?/g);

  for (const m of numMatches) {

    const n = parseFloat(m[0].replace(/,/g, ''));

    if (!isNaN(n) && Number.isInteger(n)) nums.push(n);

  }

  if (nums.length < 4) throw new Error(`Not enough numbers after Non-Fatal Shooting: ${nums.join(',')}`);



  // Layout: priorDay, prior7Days, ytd_current, ytd_prior

  return { ytd: nums[2], prior: nums[3], asof };

}



// ─── Durham ───────────────────────────────────────────────────────────────────



async function fetchDurham() {

  // Durham PDF contains an image-based bar chart - send PDF directly to Claude vision API

  const archiveUrl = 'https://www.durhamnc.gov/Archive.aspx?AMID=211';

  console.log('Durham archive URL:', archiveUrl);

  const archResp = await fetchUrl(archiveUrl);

  if (archResp.status !== 200) throw new Error(`Durham archive HTTP ${archResp.status}`);



  const html = archResp.body.toString('utf8');

  const adidMatches = [...html.matchAll(/ADID=(\d+)/g)].map(m => parseInt(m[1]));

  if (!adidMatches.length) throw new Error('No ADID links found');

  const latestAdid = Math.max(...adidMatches);

  const pdfUrl = `https://www.durhamnc.gov/ArchiveCenter/ViewFile/Item/${latestAdid}`;

  console.log('Durham PDF URL:', pdfUrl, '(ADID:', latestAdid + ')');



  const pdfResp = await fetchUrl(pdfUrl);

  if (pdfResp.status !== 200) throw new Error(`Durham PDF HTTP ${pdfResp.status}`);



  // Get as-of date from PDF text layer

  const pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js'));

  pdfjsLib.GlobalWorkerOptions.workerSrc = false;

  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfResp.body) }).promise;

  const pg1 = await pdf.getPage(1);

  const tc  = await pg1.getTextContent();

  const text = tc.items.map(i => i.str).join(' ');

  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);

  let asof = null;

  if (dateMatch) {

    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};

    const mo = months[dateMatch[1].toLowerCase()];

    if (mo) asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;

  }

  console.log('Durham asof:', asof);



  // Send PDF directly to Claude vision API (no canvas needed)

  const base64Pdf = pdfResp.body.toString('base64');

  console.log('Durham: sending PDF to vision API, size:', pdfResp.body.length, 'bytes');



  // Send to Claude vision API — ask for both Fatal and Non-Fatal, plus as-of date
  const responseText = await callClaudeVision(base64Pdf, 'application/pdf',
    'This is a Durham Police Department shooting data chart. It has bar groups for Fatal and Non-Fatal victims. For BOTH the "Fatal" and "Non-Fatal" bar groups, what are the exact numbers shown above the bars for 2024, 2025, and 2026? Also find the "through" date (e.g. "through March 5, 2026"). Reply with ONLY: FATAL 2024=N 2025=N 2026=N NONFATAL 2024=N 2025=N 2026=N DATE=YYYY-MM-DD');

  console.log('Durham vision response:', responseText);

  // Parse fatal + non-fatal for each year
  const fatalSection = responseText.match(/FATAL\s+2024=(\d+)\s+2025=(\d+)\s+2026=(\d+)/);
  const nonfatalSection = responseText.match(/NONFATAL\s+2024=(\d+)\s+2025=(\d+)\s+2026=(\d+)/);

  // Fallback: try simple year=N pattern if structured format not found
  const m2025 = responseText.match(/2025=(\d+)/g);
  const m2026 = responseText.match(/2026=(\d+)/g);

  let ytd, prior;
  if (fatalSection && nonfatalSection) {
    ytd = parseInt(fatalSection[3]) + parseInt(nonfatalSection[3]);
    prior = parseInt(fatalSection[2]) + parseInt(nonfatalSection[2]);
    console.log('Durham: fatal 2026=' + fatalSection[3] + ' nonfatal 2026=' + nonfatalSection[3] + ' total=' + ytd);
  } else if (m2026 && m2026.length >= 2) {
    // Two matches = fatal + nonfatal
    const vals2026 = m2026.map(s => parseInt(s.match(/\d+$/)[0]));
    ytd = vals2026.reduce((a, b) => a + b, 0);
    const vals2025 = m2025 ? m2025.map(s => parseInt(s.match(/\d+$/)[0])) : [];
    prior = vals2025.reduce((a, b) => a + b, 0) || null;
    console.log('Durham: parsed 2026 values:', vals2026, 'total=' + ytd);
  } else if (m2026) {
    ytd = parseInt(m2026[0].match(/\d+$/)[0]);
    prior = m2025 ? parseInt(m2025[0].match(/\d+$/)[0]) : null;
  } else {
    throw new Error('Could not parse Durham chart values. Response: ' + responseText);
  }

  // Use vision-extracted date if text layer didn't have it
  if (!asof) {
    const dateFromVision = responseText.match(/DATE=(\d{4}-\d{2}-\d{2})/);
    if (dateFromVision) {
      asof = dateFromVision[1];
      console.log('Durham: got asof from vision:', asof);
    }
  }

  return { ytd, prior, asof };

}





// ─── Milwaukee (Tableau) ──────────────────────────────────────────────────────



async function fetchMilwaukee() {

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  page.setDefaultTimeout(30000);



  console.log('Milwaukee: loading Tableau dashboard...');

  await page.goto(

    'https://public.tableau.com/views/MilwaukeePoliceDepartment-PartICrimes/MPDPublicCrimeDashboard?:embed=y&:showVizHome=no',

    { waitUntil: 'domcontentloaded', timeout: 60000 }

  );



  // Wait for dashboard to render

  try {

    await page.waitForFunction(

      () => document.body.innerText.includes('Non-Fatal'),

      { timeout: 30000 }

    );

  } catch(e) {

    console.log('Milwaukee: Non-Fatal not found after 30s, proceeding anyway...');

  }

  await page.waitForTimeout(5000);



  // Get as-of date

  const fullText = await page.evaluate(() => document.body.innerText);

  const dateMatch = fullText.match(/Data Current Through[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  let asof = null;

  if (dateMatch) {

    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;

  }

  console.log('Milwaukee asof:', asof);



  // Screenshot the page and send to Claude vision API

  const screenshotBuf = await page.screenshot({ fullPage: false });

  await browser.close();

  console.log('Milwaukee: screenshot taken, size:', screenshotBuf.length, 'bytes');



  const base64Image = screenshotBuf.toString('base64');

  const responseText = await callClaudeVision(base64Image, 'image/png',
    'This is a Milwaukee Police Department crime dashboard. Find the row labeled "Non-Fatal Shooting" in the table. It has columns for YTD 2024, YTD 2025, and YTD 2026. What are those three YTD numbers? Reply with ONLY: YTD2024=N YTD2025=N YTD2026=N');

  console.log('Milwaukee vision response:', responseText);



  const m2025 = responseText.match(/YTD2025=(\d+)/);

  const m2026 = responseText.match(/YTD2026=(\d+)/);



  if (!m2026) throw new Error('Could not parse Milwaukee YTD from vision API. Response: ' + responseText);



  return {

    ytd:   parseInt(m2026[1]),

    prior: m2025 ? parseInt(m2025[1]) : null,

    asof

  };

}





// ─── Memphis (Power BI API) ───────────────────────────────────────────────────

async function fetchMemphis() {
  console.log('Memphis: querying Power BI API...');
  const yr = new Date().getFullYear();
  const dsr = await pbiQuery(
    'wabi-us-gov-virginia-api.analysis.usgovcloudapi.net',
    'e62bd4cd-e346-4e1b-8d23-961afb9e2d58',
    1354500, 'edf40fb7-0f73-4122-a76f-fb7561bb2998',
    {
      Version: 2,
      From: [
        { Name: 'c', Entity: 'Crime Measure Report Table', Type: 0 },
        { Name: 'd', Entity: 'Offense Calendar', Type: 0 }
      ],
      Select: [
        { Column: { Expression: { SourceRef: { Source: 'd' } }, Property: 'Year' }, Name: 'Year' },
        { Measure: { Expression: { SourceRef: { Source: 'c' } }, Property: 'Inc Shoot YTD' }, Name: 'ShootYTD' }
      ]
    }
  );
  const counts = parsePbiYearCounts(dsr);
  const ytd = counts[yr];
  const prior = counts[yr - 1];
  if (ytd == null) throw new Error('Memphis: no data for ' + yr + '. Years found: ' + Object.keys(counts).join(', '));
  const asof = await pbiAsOf(
    'wabi-us-gov-virginia-api.analysis.usgovcloudapi.net',
    'e62bd4cd-e346-4e1b-8d23-961afb9e2d58',
    1354500, 'edf40fb7-0f73-4122-a76f-fb7561bb2998',
    {
      Version: 2,
      From: [{ Name: 'c', Entity: 'Crime Measure Report Table', Type: 0 }],
      Select: [
        { Measure: { Expression: { SourceRef: { Source: 'c' } }, Property: 'Yesterday' }, Name: 'Yesterday' }
      ]
    },
    'Memphis'
  );
  console.log('Memphis: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}





// ─── Pittsburgh (Power BI Gov) ───────────────────────────────────────────────



async function fetchPittsburgh() {

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  await page.setViewportSize({ width: 1536, height: 768 });

  page.setDefaultTimeout(30000);



  const url = 'https://app.powerbigov.us/view?r=eyJrIjoiMDYzNWMyNGItNWNjMS00ODMwLWIxZDgtMTNkNzhlZDE2OWFjIiwidCI6ImY1ZjQ3OTE3LWM5MDQtNDM2OC05MTIwLWQzMjdjZjE3NTU5MSJ9';

  console.log('Pittsburgh: loading Power BI dashboard...');

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

  await waitForPowerBI(page, 15000);

  const page1Text = await page.evaluate(() => document.body.innerText);

  const dateMatch = page1Text.match(/Last Updated[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  let asof = null;

  if (dateMatch) {

    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;

  }

  console.log('Pittsburgh asof:', asof);

  console.log('Pittsburgh page1 snippet:', page1Text.substring(0, 400));



  console.log('Pittsburgh: navigating to Year to Date Stats page...');

  let navigated = false;

  for (const selector of [

    '[aria-label="Year to Date Stats"]',

    '[aria-label="Annual Stats"]',

    'button.sectionItem:first-child',

    '.pbi-glyph-chevronrightmedium',

  ]) {

    try {

      await page.locator(selector).first().click({ force: true, timeout: 5000 });

      await page.waitForTimeout(8000);

      console.log('Pittsburgh: navigated via', selector);

      navigated = true;

      break;

    } catch(e) { /* try next */ }

  }

  if (!navigated) console.log('Pittsburgh: could not navigate, will screenshot current page');



  const page2Text = await page.evaluate(() => document.body.innerText);

  console.log('Pittsburgh post-nav snippet:', page2Text.substring(0, 400));

  // Retry date extraction from page 2 if page 1 missed it
  if (!asof) {
    const dateMatch2 = page2Text.match(/Last Updated[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
    if (dateMatch2) {
      asof = `${dateMatch2[3]}-${dateMatch2[1].padStart(2,'0')}-${dateMatch2[2].padStart(2,'0')}`;
      console.log('Pittsburgh asof (from page 2):', asof);
    }
  }

  // Final fallback: any MM/DD/YYYY date in the page
  if (!asof) {
    const anyDate = page2Text.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (anyDate) {
      asof = `${anyDate[3]}-${anyDate[1].padStart(2,'0')}-${anyDate[2].padStart(2,'0')}`;
      console.log('Pittsburgh asof (fallback):', asof);
    }
  }

  const pageText = await page.evaluate(() => document.body.innerText);

  await browser.close();



  const yr = new Date().getFullYear();



  let homYtd = null, homPrior = null, nfsYtd = null, nfsPrior = null;



  const homSection = pageText.match(/Number of Homicides[\s\S]*?Number of Non-Fatal/);

  if (homSection) {

    const rows = homSection[0].matchAll(/(\d{4})\n(\d+)\n/g);

    for (const r of rows) {

      if (parseInt(r[1]) === yr)     homYtd   = parseInt(r[2]);

      if (parseInt(r[1]) === yr - 1) homPrior = parseInt(r[2]);

    }

  }



  const nfsSection = pageText.match(/Number of Non-Fatal Shootings[\s\S]*?(?:Last 28|YTD %|$)/);

  if (nfsSection) {

    const rows = nfsSection[0].matchAll(/(\d{4})\n(\d+)\n/g);

    for (const r of rows) {

      if (parseInt(r[1]) === yr)     nfsYtd   = parseInt(r[2]);

      if (parseInt(r[1]) === yr - 1) nfsPrior = parseInt(r[2]);

    }

  }



  if (homYtd === null || nfsYtd === null) {

    const allRows = [...pageText.matchAll(/Select Row\s+(\d{4})\s+(\d+)\s+[-\d.]+%/g)];

    console.log('Pittsburgh fallback rows:', allRows.map(r => `${r[1]}=${r[2]}`).join(', '));

    const yrRows = allRows.filter(r => parseInt(r[1]) === yr);

    const priorRows = allRows.filter(r => parseInt(r[1]) === yr - 1);

    if (yrRows.length >= 2) {

      homYtd = parseInt(yrRows[0][2]);

      nfsYtd = parseInt(yrRows[1][2]);

    }

    if (priorRows.length >= 2) {

      homPrior = parseInt(priorRows[0][2]);

      nfsPrior = parseInt(priorRows[1][2]);

    }

  }



  console.log(`Pittsburgh parsed: hom${yr}=${homYtd} nfs${yr}=${nfsYtd} hom${yr-1}=${homPrior} nfs${yr-1}=${nfsPrior}`);



  if (homYtd === null || nfsYtd === null) {

    throw new Error('Could not parse Pittsburgh homicide/NFS values from page text');

  }



  return {

    ytd:   homYtd + nfsYtd,

    prior: (homPrior !== null && nfsPrior !== null) ? homPrior + nfsPrior : null,

    asof

  };

}





// ─── NY GIVE Dashboard (Tableau - shared across Albany, Buffalo, Syracuse, Suffolk County) ───

// Single browser session downloads CSV for each city sequentially, results cached.

const GIVE_CITIES = {
  albany:        { jurisdiction: 'Albany City PD',   label: 'Albany' },
  buffalo:       { jurisdiction: 'Buffalo City PD',  label: 'Buffalo' },
  syracuse:      { jurisdiction: 'Syracuse City PD', label: 'Syracuse' },
  suffolkcounty: { jurisdiction: 'Suffolk County PD', label: 'Suffolk County' },
};

let _givePromise = null;
function fetchGIVEAll() {
  if (!_givePromise) _givePromise = _fetchGIVEAllImpl();
  return _givePromise;
}

function parseGIVECsv(csvText, cityLabel) {
  const yr = new Date().getFullYear();
  const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const currSuffix = '-' + String(yr).slice(2);
  const priorSuffix = '-' + String(yr - 1).slice(2);
  const currMonths = new Set(monthNames.map(m => m + currSuffix));

  const rows = csvText.split('\n').map(function(l) { return l.replace(/\r/g, '').trim(); }).filter(Boolean);
  console.log(cityLabel + ': total rows:', rows.length);

  // Find latest current-year month
  let maxMonthIdx = -1;
  for (var i = 1; i < rows.length; i++) {
    var cols = rows[i].split('\t');
    if (cols.length < 3) continue;
    var month = cols[0].trim();
    if (currMonths.has(month)) {
      var moIdx = monthNames.indexOf(month.split('-')[0]);
      if (moIdx > maxMonthIdx) maxMonthIdx = moIdx;
    }
  }

  const priorMonths = new Set();
  for (var mi = 0; mi <= maxMonthIdx; mi++) {
    priorMonths.add(monthNames[mi] + priorSuffix);
  }

  // Deduplicate rows (Tableau crosstab exports duplicate rows)
  var seen = new Set();
  var dedupedRows = [rows[0]];
  for (var i = 1; i < rows.length; i++) {
    if (!seen.has(rows[i])) { seen.add(rows[i]); dedupedRows.push(rows[i]); }
  }

  let ytd = 0, prior = 0, latestMonth = null, foundAnyCurr = false;

  for (var i = 1; i < dedupedRows.length; i++) {
    var cols = dedupedRows[i].split('\t');
    if (cols.length < 3) continue;
    var month    = cols[0].trim();
    var category = cols[1].trim().toLowerCase();
    var count    = parseInt(cols[2].trim().replace(/,/g, ''));
    if (isNaN(count)) continue;

    if (category.indexOf('shooting victims') < 0 && category.indexOf('persons hit') < 0 &&
        category.indexOf('individuals killed') < 0 && category.indexOf('gun violence') < 0) continue;

    if (currMonths.has(month)) {
      foundAnyCurr = true;
      ytd += count;
      latestMonth = month;
    }
    if (priorMonths.has(month)) {
      prior += count;
    }
  }

  console.log(cityLabel + ' parsed: ytd=' + ytd + ' prior=' + prior + ' latestMonth=' + latestMonth);

  if (!foundAnyCurr) {
    throw new Error(cityLabel + ': no current year data found');
  }

  var asofDate = yr + '-01-31';
  if (latestMonth) {
    var moIdx = monthNames.indexOf(latestMonth.split('-')[0]);
    if (moIdx >= 0) {
      var lastDay = new Date(yr, moIdx + 1, 0).getDate();
      asofDate = yr + '-' + String(moIdx + 1).padStart(2, '0') + '-' + String(lastDay).padStart(2, '0');
    }
  }

  return { ytd, prior, asof: asofDate };
}

async function _fetchGIVESingleCity(browser, key, city) {
  const page = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 1024 });
  page.setDefaultTimeout(30000);

  try {
    const url = 'https://mypublicdashboard.ny.gov/t/OJRP_PUBLIC/views/GIVEShootingActivity/ShootingActivity?Jurisdiction=' + encodeURIComponent(city.jurisdiction);
    console.log(city.label + ': loading...');
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForTimeout(12000);

    // Switch to Monthly Data
    console.log(city.label + ': clicking Monthly Data...');
    await page.locator('text=Monthly Data').first().click({ force: true });
    await page.waitForTimeout(6000);

    // Download → Crosstab → Monthly Total Overview → CSV
    console.log(city.label + ': downloading CSV...');
    await page.locator('[data-tb-test-id="viz-viewer-toolbar-button-download"]').first().click({ force: true });
    await page.waitForTimeout(2000);

    await page.locator('div').filter({ hasText: /^Crosstab$/ }).first().click({ force: true });
    await page.waitForTimeout(2000);

    try {
      await page.locator('text=Monthly Total Overview').first().click({ force: true, timeout: 5000 });
      await page.waitForTimeout(1000);
    } catch(e) { /* may already be selected */ }

    try {
      await page.locator('text=CSV').first().click({ force: true, timeout: 5000 });
      await page.waitForTimeout(500);
    } catch(e) { /* may already be selected */ }

    var [ download ] = await Promise.all([
      page.waitForEvent('download', { timeout: 30000 }),
      page.locator('button:has-text("Download")').last().click({ force: true })
    ]);
    var stream = await download.createReadStream();
    var chunks = [];
    await new Promise(function(res, rej) {
      stream.on('data', function(c) { chunks.push(c); });
      stream.on('end', res);
      stream.on('error', rej);
    });
    var buf = Buffer.concat(chunks);
    var csvText = buf.toString('utf16le').replace(/^\uFEFF/, '');
    console.log(city.label + ': CSV downloaded, bytes:', buf.length);

    var result = parseGIVECsv(csvText, city.label);
    console.log(city.label + ': OK — ytd=' + result.ytd + ' prior=' + result.prior);
    return result;
  } catch(e) {
    console.log(city.label + ': FAILED — ' + e.message);
    return null;
  } finally {
    await page.close();
  }
}

async function _fetchGIVEAllImpl() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });

  // Process all 4 cities in parallel (separate pages, shared browser)
  console.log('GIVE: launching all 4 cities in parallel...');
  const cityKeys = Object.keys(GIVE_CITIES);
  const promises = cityKeys.map(key => _fetchGIVESingleCity(browser, key, GIVE_CITIES[key]));
  const resultArr = await Promise.all(promises);

  const results = {};
  cityKeys.forEach((key, i) => { results[key] = resultArr[i]; });

  await browser.close();
  return results;
}

async function fetchBuffalo() {
  var all = await fetchGIVEAll();
  if (!all.buffalo) throw new Error('Buffalo: no data from GIVE session');
  return all.buffalo;
}

async function fetchAlbany() {
  var all = await fetchGIVEAll();
  if (!all.albany) throw new Error('Albany: no data from GIVE session');
  return all.albany;
}

async function fetchSyracuse() {
  var all = await fetchGIVEAll();
  if (!all.syracuse) throw new Error('Syracuse: no data from GIVE session');
  return all.syracuse;
}

async function fetchSuffolkCounty() {
  var all = await fetchGIVEAll();
  if (!all.suffolkcounty) throw new Error('Suffolk County: no data from GIVE session');
  return all.suffolkcounty;
}





async function fetchMiamiDade() {
  console.log('MiamiDade: querying Power BI API...');
  const yr = new Date().getFullYear();
  const dsr = await pbiQuery(
    'wabi-us-gov-virginia-api.analysis.usgovcloudapi.net',
    '41dbd6d3-ff55-499e-a800-48116bebaa28',
    1568733, '18dcb4b6-5a32-4488-abff-b2eb04d60f5e',
    {
      Version: 2,
      From: [
        { Name: 'n', Entity: 'NIBRS_D', Type: 0 },
        { Name: 'c', Entity: 'CALENDAR_TABLE', Type: 0 },
        { Name: 'm', Entity: 'Measure Table', Type: 0 }
      ],
      Select: [
        { Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'YEAR' }, Name: 'Year' },
        { Measure: { Expression: { SourceRef: { Source: 'm' } }, Property: 'YTD_TOTALCRIME' }, Name: 'YTD' }
      ],
      Where: [{
        Condition: {
          In: {
            Expressions: [{ Column: { Expression: { SourceRef: { Source: 'n' } }, Property: 'PUBLIC DEFINITION' } }],
            Values: [[{ Literal: { Value: "'SHOOTINGS'" } }]]
          }
        }
      }]
    }
  );
  const counts = parsePbiYearCounts(dsr);
  const ytd = counts[yr];
  const prior = counts[yr - 1];
  if (ytd == null) throw new Error('MiamiDade: no data for ' + yr + '. Years: ' + Object.keys(counts).join(', '));
  const asof = await pbiAsOf(
    'wabi-us-gov-virginia-api.analysis.usgovcloudapi.net',
    '41dbd6d3-ff55-499e-a800-48116bebaa28',
    1568733, '18dcb4b6-5a32-4488-abff-b2eb04d60f5e',
    {
      Version: 2,
      From: [{ Name: 'x', Entity: 'CDWT_CRIME', Type: 0 }],
      Select: [
        { Aggregation: { Expression: { Column: { Expression: { SourceRef: { Source: 'x' } }, Property: 'INCIDENT_FROM_DATE' } }, Function: 4 }, Name: 'MaxIncidentDate' }
      ]
    },
    'MiamiDade'
  );
  console.log('MiamiDade: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}


// ─── Las Vegas (LVMPD Weekly Crime Report PDF) ──────────────────────────────

function decodeHtmlEntities(s) {
  return String(s || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function stripHtml(s) {
  return decodeHtmlEntities(String(s || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim());
}

function absoluteUrl(href, baseUrl) {
  try { return new URL(decodeHtmlEntities(href), baseUrl).href; }
  catch { return null; }
}

function findVegasCrimeReportLink(html) {
  const links = [];
  const re = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(html))) {
    const href = absoluteUrl(m[1], 'https://www.lvmpd.com/about/transparency/statistics');
    const text = stripHtml(m[2]).toLowerCase();
    const hrefLower = String(href || '').toLowerCase();
    if (!href) continue;
    links.push({ href, text, hrefLower });
  }

  const crimeReport = links.find(l =>
    l.text.includes('crime report') &&
    (l.hrefLower.includes('.pdf') || l.hrefLower.includes('/showpublisheddocument/'))
  );
  if (crimeReport) return crimeReport.href;

  const fallback = links.find(l =>
    (l.text.includes('crime report') || l.hrefLower.includes('crime')) &&
    (l.hrefLower.includes('.pdf') || l.hrefLower.includes('/showpublisheddocument/'))
  );
  return fallback ? fallback.href : null;
}

function findVegasCrimeReportLinkFromMarkdown(text) {
  const m = String(text || '').match(/\[Crime Report[^\]]*?\]\((https:\/\/www\.lvmpd\.com\/home\/showpublisheddocument\/[^)]+)\)/i);
  return m ? m[1] : null;
}

function readerUrl(targetUrl) {
  return 'https://r.jina.ai/http://r.jina.ai/http://' + targetUrl;
}

function parseVegasReportText(text) {
  const body = String(text || '').replace(/\s+/g, ' ');
  const row = body.match(/Shooting Victims\s+(-?[\d,]+(?:\.\d+)?%?)\s+(-?[\d,]+(?:\.\d+)?%?)\s+(-?[\d,]+(?:\.\d+)?%?)\s+(-?[\d,]+(?:\.\d+)?%?)/i);
  if (!row) throw new Error('Vegas reader: Shooting Victims row not found');

  const ytd = parseInt(row[1].replace(/,/g, ''), 10);
  const prior = parseInt(row[3].replace(/,/g, ''), 10);
  if (!Number.isFinite(ytd) || !Number.isFinite(prior)) {
    throw new Error('Vegas reader: could not parse Shooting Victims counts');
  }

  let asof = null;
  const dateMatch = body.match(/[Ww]eek\s+[Ee]nding:?\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (dateMatch) {
    asof = dateMatch[3] + '-' + String(parseInt(dateMatch[1], 10)).padStart(2, '0') + '-' + String(parseInt(dateMatch[2], 10)).padStart(2, '0');
  }
  if (!asof) throw new Error('Vegas reader: week ending date not found');

  return { ytd, prior, asof };
}

async function fetchVegasViaReader(pdfLink) {
  let reportLink = pdfLink;
  if (!reportLink) {
    const statsResp = await fetchUrlRetry(readerUrl('https://www.lvmpd.com/about/transparency/statistics'), {
      label: 'Vegas reader stats page',
      attempts: 2,
      timeoutMs: 30000
    });
    reportLink = findVegasCrimeReportLinkFromMarkdown(statsResp.body.toString('utf8'));
    if (!reportLink) throw new Error('Vegas reader: crime report link not found');
  }

  console.log('Vegas: using reader fallback for', reportLink);
  const reportResp = await fetchUrlRetry(readerUrl(reportLink), {
    label: 'Vegas reader report',
    attempts: 2,
    timeoutMs: 45000
  });
  const result = parseVegasReportText(reportResp.body.toString('utf8'));
  console.log('Vegas reader: ytd=' + result.ytd + ' prior=' + result.prior + ' asof=' + result.asof);
  return result;
}

async function downloadPdfWithBrowser(page, pdfLink) {
  const downloadPromise = page.waitForEvent('download', { timeout: 15000 }).catch(() => null);
  const response = await page.goto(pdfLink, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(e => {
    console.log('Vegas: browser PDF navigation failed:', e.message);
    return null;
  });
  const download = await downloadPromise;
  if (download) {
    const downloadPath = await download.path();
    if (!downloadPath) throw new Error('Vegas: PDF download path null');
    return require('fs').readFileSync(downloadPath);
  }
  if (response && response.ok()) {
    return await response.body();
  }
  throw new Error('Vegas: PDF request failed' + (response ? ' HTTP ' + response.status() : ''));
}

async function fetchVegas() {
  const statsUrl = 'https://www.lvmpd.com/about/transparency/statistics';
  let pdfLink = null;

  try {
    const statsResp = await fetchUrlRetry(statsUrl, { label: 'Vegas stats page', attempts: 2, timeoutMs: 30000 });
    const statsHtml = statsResp.body.toString('utf8');
    if (!/Access Denied/i.test(statsHtml)) {
      pdfLink = findVegasCrimeReportLink(statsHtml);
      if (pdfLink) console.log('Vegas: found PDF from static HTML:', pdfLink);
    }
  } catch (e) {
    console.log('Vegas: static HTML lookup failed:', e.message);
  }

  const { chromium } = require('playwright');
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled']
  });

  try {
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      viewport: { width: 1920, height: 1080 },
      acceptDownloads: true
    });
    const page = await context.newPage();
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });

    if (!pdfLink) {
      console.log('Vegas: navigating to statistics page...');
      await page.goto(statsUrl, {
        waitUntil: 'domcontentloaded', timeout: 30000
      });

      // Find the crime report PDF link
      pdfLink = await page.evaluate(() => {
        const links = Array.from(document.querySelectorAll('a[href]'));
        for (const a of links) {
          const text = a.textContent.toLowerCase();
          const href = a.href.toLowerCase();
          if (text.includes('crime report') &&
              (href.includes('.pdf') || href.includes('/showpublisheddocument/'))) {
            return a.href;
          }
        }
        // Fallback: any PDF/published document link with 'crime' in text or URL
        for (const a of links) {
          const text = a.textContent.toLowerCase();
          const href = a.href.toLowerCase();
          if ((href.includes('.pdf') || href.includes('/showpublisheddocument/')) &&
              (text.includes('crime') || href.includes('crime'))) {
            return a.href;
          }
        }
        return null;
      });
    }

    if (!pdfLink) {
      // Log all links for debugging
      const allLinks = await page.evaluate(() =>
        Array.from(document.querySelectorAll('a[href]'))
          .map(a => ({ href: a.href, text: a.textContent.trim().substring(0, 80) }))
          .filter(l => l.href.includes('.pdf') || l.text.toLowerCase().includes('report'))
      );
      console.log('Vegas: no crime report PDF found. Links:', JSON.stringify(allLinks));
      pdfLink = 'https://www.lvmpd.com/home/showpublisheddocument/8456';
      console.log('Vegas: falling back to stable crime report document URL:', pdfLink);
    }

    console.log('Vegas: downloading PDF from', pdfLink);

    let pdfBuffer;
    try {
      pdfBuffer = await downloadPdfWithBrowser(page, pdfLink);
    } catch (e) {
      console.log('Vegas: direct PDF download failed:', e.message);
      await browser.close();
      return await fetchVegasViaReader(pdfLink);
    }

    if (pdfBuffer.length < 5000 || pdfBuffer[0] !== 0x25) {
      throw new Error('Vegas: downloaded file is not a valid PDF (' + pdfBuffer.length + ' bytes)');
    }
    console.log('Vegas: PDF downloaded (' + (pdfBuffer.length / 1024).toFixed(0) + ' KB)');

    await browser.close();

    // Parse page 2 for shooting victims
    const tokens = await extractPdfTokens(pdfBuffer, 2);
    const joined = tokens.join(' ');
    console.log('Vegas: page 2 tokens (first 500):', joined.substring(0, 500));

    // Look for "Shooting Victims" row — extract YTD and prior year numbers
    // Typical format: "Shooting Victims  <weekly> <ytd_current> <ytd_prior> ..."
    const shootingMatch = joined.match(/Shooting\s*Victims?[\s\S]*?(?=\n|Robbery|Domestic|Sexual|Homicide|Auto|Vehicle|Total|$)/i);
    if (!shootingMatch) throw new Error('Vegas: "Shooting Victims" not found on page 2. Tokens: ' + tokens.slice(0, 80).join('|'));

    const afterLabel = shootingMatch[0].replace(/Shooting\s*Victims?/i, '').trim();
    const nums = [];
    const numMatches = afterLabel.matchAll(/-?[\d,]+/g);
    for (const m of numMatches) {
      const n = parseInt(m[0].replace(/,/g, ''));
      if (!isNaN(n)) nums.push(n);
    }

    console.log('Vegas: extracted numbers after "Shooting Victims":', nums);

    if (nums.length < 2) throw new Error('Vegas: not enough numbers after Shooting Victims: ' + nums.join(','));

    // Columns on the LVMPD statistical report are:
    // Current YTD Reported, Current YTD Arrested, Previous YTD Reported, Previous YTD Arrested,
    // Percent Change Reported, Percent Change Arrested. Shooting victims use Reported counts.
    let ytd, prior;
    if (nums.length >= 4) {
      ytd = nums[0];
      prior = nums[2];
    } else {
      ytd = nums[0];
      prior = nums[1];
    }

    // Sanity check — YTD should be larger than weekly
    if (ytd < 5 || prior < 5) {
      console.log('Vegas: warning — values seem too low, trying alternative column positions');
      // Try last two numbers
      ytd = nums[nums.length - 2];
      prior = nums[nums.length - 1];
    }

    // Extract "week ending" date from PDF text for asof
    const page1Tokens = await extractPdfTokens(pdfBuffer, 1);
    const page1Text = page1Tokens.join(' ');
    let asof = null;
    const numericDateMatch = page1Text.match(/[Ww]eek\s+[Ee]nding:?\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (numericDateMatch) {
      asof = numericDateMatch[3] + '-' + String(parseInt(numericDateMatch[1])).padStart(2,'0') + '-' + String(parseInt(numericDateMatch[2])).padStart(2,'0');
    }
    const dateMatch = !asof && page1Text.match(/[Ww]eek\s+[Ee]nding\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/);
    if (dateMatch) {
      const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
      const mo = months[dateMatch[1].toLowerCase()];
      if (mo) asof = dateMatch[3] + '-' + String(mo).padStart(2,'0') + '-' + String(parseInt(dateMatch[2])).padStart(2,'0');
    }
    if (!asof) {
      // Try page 2
      const numericDateMatch2 = joined.match(/[Ww]eek\s+[Ee]nding:?\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      if (numericDateMatch2) {
        asof = numericDateMatch2[3] + '-' + String(parseInt(numericDateMatch2[1])).padStart(2,'0') + '-' + String(parseInt(numericDateMatch2[2])).padStart(2,'0');
      }
      const dateMatch2 = !asof && joined.match(/[Ww]eek\s+[Ee]nding\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/);
      if (dateMatch2) {
        const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
        const mo = months[dateMatch2[1].toLowerCase()];
        if (mo) asof = dateMatch2[3] + '-' + String(mo).padStart(2,'0') + '-' + String(parseInt(dateMatch2[2])).padStart(2,'0');
      }
    }
    if (!asof) {
      // Fallback: use today minus 3 days
      const d = new Date(); d.setDate(d.getDate() - 3);
      asof = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    }

    console.log('Vegas: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
    return { ytd, prior, asof };

  } catch(e) {
    await browser.close().catch(() => {});
    throw e;
  }
}



// ─── Chicago (Socrata) ──────────────────────────────────────────────────────

async function fetchChicago() {
  const base = 'https://data.cityofchicago.org/resource/gumc-mgzr.json';
  const CURRENT_YEAR = new Date().getFullYear();

  async function socrataCount(where) {
    const url = base + '?$where=' + encodeURIComponent(where) + '&$select=count(*)%20as%20n&$limit=1';
    const d = await fetchJsonRetry(url, { label: 'Chicago count', attempts: 3, timeoutMs: 45000 });
    return parseInt(d[0] && d[0].n ? d[0].n : 0);
  }

  // Latest date
  const latestUrl = base + '?$order=date%20DESC&$limit=1&$select=date&$where=' + encodeURIComponent("gunshot_injury_i = 'YES'");
  const latestData = await fetchJsonRetry(latestUrl, { label: 'Chicago latest', attempts: 3, timeoutMs: 45000 });
  const asof = latestData[0] && latestData[0].date ? latestData[0].date.slice(0, 10) : null;
  if (!asof) throw new Error('Chicago: no latest date');

  const ytdWhere = "date >= '" + CURRENT_YEAR + "-01-01T00:00:00.000' AND date <= '" + asof + "T23:59:59.000' AND gunshot_injury_i = 'YES'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = "date >= '" + (CURRENT_YEAR - 1) + "-01-01T00:00:00.000' AND date <= '" + priorEnd + "T23:59:59.000' AND gunshot_injury_i = 'YES'";

  const [ytd, prior] = await Promise.all([socrataCount(ytdWhere), socrataCount(priorWhere)]);
  console.log('Chicago: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Baltimore (ArcGIS) ─────────────────────────────────────────────────────

async function fetchBaltimore() {
  const base = 'https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/NIBRS_GroupA_Crime_Data/FeatureServer/0/query';
  const CURRENT_YEAR = new Date().getFullYear();
  const gunWeapons = [
    'AUTOMATIC_FIREARM',
    'AUTOMATIC_HANDGUN',
    'AUTOMATIC_OTHER_FIREARM',
    'AUTOMATIC_RIFLE',
    'AUTOMATIC_SHOTGUN',
    'FIREARM',
    'HANDGUN',
    'OTHER_FIREARM',
    'RIFLE',
    'SHOTGUN'
  ];
  const shootingVictimFilter = "(Shooting = 'Y' OR (Description = 'HOMICIDE' AND Weapon IN (" +
    gunWeapons.map(w => "'" + w + "'").join(',') + ')))';

  async function arcCount(where) {
    const url = base + '?where=' + encodeURIComponent(where) + '&returnCountOnly=true&f=json';
    const d = await fetchJsonRetry(url, { label: 'Baltimore count', attempts: 3, timeoutMs: 45000 });
    if (d.error) throw new Error('Baltimore: ' + (d.error.message || JSON.stringify(d.error).slice(0, 120)));
    return d.count;
  }

  // Latest
  const latestUrl = base + '?where=' + encodeURIComponent(shootingVictimFilter) +
    '&outFields=CrimeDateTime&orderByFields=CrimeDateTime+DESC&resultRecordCount=1&returnGeometry=false&f=json';
  const latestData = await fetchJsonRetry(latestUrl, { label: 'Baltimore latest', attempts: 3, timeoutMs: 45000 });
  if (latestData.error) throw new Error('Baltimore: ' + (latestData.error.message || JSON.stringify(latestData.error).slice(0, 120)));
  let asof = null;
  if (latestData.features && latestData.features.length) {
    const raw = latestData.features[0].attributes.CrimeDateTime;
    if (typeof raw === 'number') {
      const dt = new Date(raw);
      asof = dt.getFullYear() + '-' + String(dt.getMonth()+1).padStart(2,'0') + '-' + String(dt.getDate()).padStart(2,'0');
    }
  }
  if (!asof) throw new Error('Baltimore: no latest date');

  const ytdWhere = shootingVictimFilter + " AND CrimeDateTime >= DATE '" + CURRENT_YEAR + "-01-01' AND CrimeDateTime <= DATE '" + asof + "'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = shootingVictimFilter + " AND CrimeDateTime >= DATE '" + (CURRENT_YEAR - 1) + "-01-01' AND CrimeDateTime <= DATE '" + priorEnd + "'";

  const [ytd, prior] = await Promise.all([arcCount(ytdWhere), arcCount(priorWhere)]);
  console.log('Baltimore: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Louisville (ArcGIS) ────────────────────────────────────────────────────

async function fetchLouisville() {
  const base = 'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Gun_Violence_Data/FeatureServer/0/query';
  const CURRENT_YEAR = new Date().getFullYear();

  async function arcCount(where) {
    const url = base + '?where=' + encodeURIComponent(where) + '&returnCountOnly=true&f=json';
    const resp = await fetchUrl(url); if (resp.status !== 200) throw new Error('Louisville: HTTP ' + resp.status);
    const d = JSON.parse(resp.body.toString('utf8'));
    if (d.error) throw new Error('Louisville: ' + (d.error.message || JSON.stringify(d.error).slice(0, 120)));
    return d.count;
  }

  const crimeFilter = "(Crime_Type = 'Non-Fatal Shooting' OR Crime_Type = 'Homicide')";
  const latestUrl = base + '?where=' + encodeURIComponent(crimeFilter) +
    '&outFields=DateTime&orderByFields=DateTime+DESC&resultRecordCount=1&f=json';
  const latestResp = await fetchUrl(latestUrl);
  const latestData = JSON.parse(latestResp.body.toString('utf8'));
  let asof = null;
  if (latestData.features && latestData.features.length) {
    const raw = latestData.features[0].attributes.DateTime;
    if (typeof raw === 'number') {
      const dt = new Date(raw);
      asof = dt.getFullYear() + '-' + String(dt.getMonth()+1).padStart(2,'0') + '-' + String(dt.getDate()).padStart(2,'0');
    }
  }
  if (!asof) throw new Error('Louisville: no latest date');

  const ytdWhere = crimeFilter + " AND DateTime >= '" + CURRENT_YEAR + "-01-01 00:00:00' AND DateTime <= '" + asof + " 23:59:59'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = crimeFilter + " AND DateTime >= '" + (CURRENT_YEAR - 1) + "-01-01 00:00:00' AND DateTime <= '" + priorEnd + " 23:59:59'";

  const [ytd, prior] = await Promise.all([arcCount(ytdWhere), arcCount(priorWhere)]);
  console.log('Louisville: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Rochester (ArcGIS) ─────────────────────────────────────────────────────

async function fetchRochester() {
  const base = 'https://services7.arcgis.com/wMvCpnbQEKXZsPSQ/arcgis/rest/services/Rochester_NY_Shooting_Victims/FeatureServer/0/query';
  const CURRENT_YEAR = new Date().getFullYear();

  async function arcCount(where) {
    const url = base + '?where=' + encodeURIComponent(where) + '&returnCountOnly=true&f=json';
    const resp = await fetchUrl(url); if (resp.status !== 200) throw new Error('Rochester: HTTP ' + resp.status);
    const d = JSON.parse(resp.body.toString('utf8'));
    if (d.error) throw new Error('Rochester: ' + (d.error.message || JSON.stringify(d.error).slice(0, 120)));
    return d.count;
  }

  const latestUrl = base + '?where=1%3D1&outFields=Occurred_Date&orderByFields=Occurred_Date+DESC&resultRecordCount=1&f=json';
  const latestResp = await fetchUrl(latestUrl);
  const latestData = JSON.parse(latestResp.body.toString('utf8'));
  let asof = null;
  if (latestData.features && latestData.features.length) {
    const raw = latestData.features[0].attributes.Occurred_Date;
    if (typeof raw === 'number') {
      const dt = new Date(raw);
      asof = dt.getFullYear() + '-' + String(dt.getMonth()+1).padStart(2,'0') + '-' + String(dt.getDate()).padStart(2,'0');
    }
  }
  if (!asof) throw new Error('Rochester: no latest date');

  const ytdWhere = "Occurred_Date >= DATE '" + CURRENT_YEAR + "-01-01' AND Occurred_Date <= DATE '" + asof + "'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = "Occurred_Date >= DATE '" + (CURRENT_YEAR - 1) + "-01-01' AND Occurred_Date <= DATE '" + priorEnd + "'";

  const [ytd, prior] = await Promise.all([arcCount(ytdWhere), arcCount(priorWhere)]);
  console.log('Rochester: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Seattle (Socrata) ──────────────────────────────────────────────────────

async function fetchSeattle() {
  const base = 'https://data.seattle.gov/resource/tazs-3rd5.json';
  const CURRENT_YEAR = new Date().getFullYear();
  const shootingFilter = "(shooting_type_group = 'Shooting (Fatal Injury)' OR shooting_type_group = 'Shooting (Non-Fatal Injury)') AND nibrs_crime_against_category = 'PERSON'";

  async function socrataCount(where) {
    const url = base + '?$where=' + encodeURIComponent(where) + '&$select=count(DISTINCT%20report_number)%20as%20n&$limit=1';
    const d = await fetchJsonRetry(url, { label: 'Seattle count', attempts: 3, timeoutMs: 45000 });
    return parseInt(d[0] && d[0].n ? d[0].n : 0);
  }

  const latestUrl = base + '?$order=offense_date%20DESC&$limit=1&$select=offense_date&$where=' + encodeURIComponent(shootingFilter);
  const latestData = await fetchJsonRetry(latestUrl, { label: 'Seattle latest', attempts: 3, timeoutMs: 45000 });
  const asof = latestData[0] && latestData[0].offense_date ? latestData[0].offense_date.slice(0, 10) : null;
  if (!asof) throw new Error('Seattle: no latest date');

  const ytdWhere = shootingFilter + " AND offense_date >= '" + CURRENT_YEAR + "-01-01T00:00:00' AND offense_date <= '" + asof + "T23:59:59'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = shootingFilter + " AND offense_date >= '" + (CURRENT_YEAR - 1) + "-01-01T00:00:00' AND offense_date <= '" + priorEnd + "T23:59:59'";

  const [ytd, prior] = await Promise.all([socrataCount(ytdWhere), socrataCount(priorWhere)]);
  console.log('Seattle: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Cincinnati (Socrata) ───────────────────────────────────────────────────

async function fetchCincinnati() {
  const base = 'https://data.cincinnati-oh.gov/resource/sfea-4ksu.json';
  const CURRENT_YEAR = new Date().getFullYear();
  function fmt(d) { return d.replace(/-/g, ''); }

  async function socrataCount(where) {
    const url = base + '?$where=' + encodeURIComponent(where) + '&$select=count(*)%20as%20n&$limit=1';
    const d = await fetchJsonRetry(url, { label: 'Cincinnati count', attempts: 3, timeoutMs: 45000 });
    return parseInt(d[0] && d[0].n ? d[0].n : 0);
  }

  const latestUrl = base + '?$order=dateoccurred%20DESC&$limit=1&$select=dateoccurred';
  const latestData = await fetchJsonRetry(latestUrl, { label: 'Cincinnati latest', attempts: 3, timeoutMs: 45000 });
  let asof = null;
  if (latestData[0] && latestData[0].dateoccurred) {
    const raw = latestData[0].dateoccurred;
    asof = raw.length === 8 ? raw.slice(0,4) + '-' + raw.slice(4,6) + '-' + raw.slice(6,8) : raw.slice(0, 10);
  }
  if (!asof) throw new Error('Cincinnati: no latest date');

  const ytdWhere = "`dateoccurred` >= '" + fmt(CURRENT_YEAR + '-01-01') + "' AND `dateoccurred` <= '" + fmt(asof) + "'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = "`dateoccurred` >= '" + fmt((CURRENT_YEAR - 1) + '-01-01') + "' AND `dateoccurred` <= '" + fmt(priorEnd) + "'";

  const [ytd, prior] = await Promise.all([socrataCount(ytdWhere), socrataCount(priorWhere)]);
  console.log('Cincinnati: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── New Orleans (Socrata CFS) ──────────────────────────────────────────────

async function fetchNewOrleans() {
  const CURRENT_YEAR = new Date().getFullYear();
  const datasets = { '2026': 'https://data.nola.gov/resource/es9j-6y5d.json', '2025': 'https://data.nola.gov/resource/4xwx-sfte.json' };
  function getUrl(year) { return datasets[String(year)] || datasets[Object.keys(datasets).sort().pop()]; }

  async function socrataCount(baseUrl, where) {
    const url = baseUrl + '?$where=' + encodeURIComponent(where) + '&$select=count(*)+as+n&$limit=1';
    const d = await fetchJsonRetry(url, { label: 'NewOrleans count', attempts: 3, timeoutMs: 45000 });
    return parseInt(d[0] && d[0].n ? d[0].n : 0);
  }

  const cfsFilter = "(type_ = '30S' OR type_ = '34S') AND disposition = 'RTF'";
  const latestUrl = getUrl(CURRENT_YEAR) + '?$order=timecreate%20DESC&$limit=1&$select=timecreate&$where=' + encodeURIComponent(cfsFilter);
  const latestData = await fetchJsonRetry(latestUrl, { label: 'NewOrleans latest', attempts: 3, timeoutMs: 45000 });
  const asof = latestData[0] && latestData[0].timecreate ? latestData[0].timecreate.slice(0, 10) : null;
  if (!asof) throw new Error('NewOrleans: no latest date');

  const ytdWhere = cfsFilter + " AND timecreate >= '" + CURRENT_YEAR + "-01-01T00:00:00.000' AND timecreate <= '" + asof + "T23:59:59.000'";
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);
  const priorWhere = cfsFilter + " AND timecreate >= '" + (CURRENT_YEAR - 1) + "-01-01T00:00:00.000' AND timecreate <= '" + priorEnd + "T23:59:59.000'";

  const [ytd, prior] = await Promise.all([
    socrataCount(getUrl(CURRENT_YEAR), ytdWhere),
    socrataCount(getUrl(CURRENT_YEAR - 1), priorWhere)
  ]);
  console.log('NewOrleans: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Boston (CSV) ───────────────────────────────────────────────────────────

async function fetchBoston() {
  const csvUrl = 'https://data.boston.gov/datastore/dump/73c7e069-701f-4910-986d-b950f46c91a1?bom=True';
  const CURRENT_YEAR = new Date().getFullYear();

  console.log('Boston: fetching CSV...');
  const resp = await fetchUrl(csvUrl, 30000);
  if (resp.status !== 200) throw new Error('Boston: HTTP ' + resp.status);
  const text = resp.body.toString('utf8');
  const lines = text.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const dateIdx = headers.findIndex(h => h.toUpperCase() === 'SHOOTING_DATE');
  if (dateIdx === -1) throw new Error('Boston: SHOOTING_DATE column not found');

  const dates = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    const val = cols[dateIdx] ? cols[dateIdx].trim().replace(/^"|"$/g, '') : '';
    if (val) dates.push(val.slice(0, 10));
  }

  dates.sort();
  const asof = dates[dates.length - 1];
  if (!asof) throw new Error('Boston: no dates found');

  const ytdStart = CURRENT_YEAR + '-01-01';
  const priorStart = (CURRENT_YEAR - 1) + '-01-01';
  const priorEnd = (CURRENT_YEAR - 1) + asof.slice(4);

  const ytd = dates.filter(d => d >= ytdStart && d <= asof).length;
  const prior = dates.filter(d => d >= priorStart && d <= priorEnd).length;

  console.log('Boston: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── NYC (NYPD CompStat PDF) ────────────────────────────────────────────────

async function fetchNYC() {
  const pdfUrl = 'https://www.nyc.gov/assets/nypd/downloads/pdf/crime_statistics/cs-en-us-city.pdf';
  console.log('NYC: fetching CompStat PDF...');
  const resp = await fetchUrl(pdfUrl, 20000);
  if (resp.status !== 200) throw new Error('NYC: HTTP ' + resp.status);

  const tokens = await extractPdfTokens(resp.body, 1);
  const text = tokens.join(' ');

  // Date range: "1/1/2026 12:00:00 AM Through 5/11/2026 12:00:00 AM"
  const dateMatch = text.match(/Through\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = dateMatch[3] + '-' + String(parseInt(dateMatch[1])).padStart(2,'0') + '-' + String(parseInt(dateMatch[2])).padStart(2,'0');
  }

  // Find "Shooting Vic" row — columns are: Week(2026,2025,%chg), 28Day(2026,2025,%chg), YTD(2026,2025,%chg), ...
  // YTD values are at indices 6 and 7 among all numeric tokens after "Shooting Vic"
  const idx = text.search(/Shooting\s*Vic/i);
  if (idx === -1) throw new Error('NYC: "Shooting Vic" not found. Tokens: ' + tokens.slice(0,60).join('|'));
  // Get text between Shooting Vic and the next crime category (Shooting Inc, Hate Crimes, Traffic, etc.)
  const rowText = text.substring(idx).split(/(?:Shooting\s*Inc|Hate\s*Crime|Traffic)/i)[0];
  const nums = rowText.match(/-?[\d,]+\.?\d*/g);
  if (!nums || nums.length < 8) throw new Error('NYC: not enough numbers in Shooting Vic row. Found: ' + (nums || []).join(', '));
  const ytd = parseInt(nums[6].replace(/,/g, ''));
  const prior = parseInt(nums[7].replace(/,/g, ''));
  console.log('NYC: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── St. Louis (CompStat PDF) ───────────────────────────────────────────────

async function fetchStLouis() {
  const pdfUrl = 'https://slmpd.org/wp-content/uploads/httpdocs/CompStat/Compstat01A.PDF?time=asap&t=' + Math.floor(Date.now() / 1000);
  console.log('StLouis: fetching CompStat PDF...');
  const resp = await fetchUrl(pdfUrl, 20000);
  if (resp.status !== 200) throw new Error('StLouis: HTTP ' + resp.status);

  const tokens = await extractPdfTokens(resp.body, 1);
  const text = tokens.join(' ');

  // Date: "to M/D/YYYY"
  const dateMatch = text.match(/to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = dateMatch[3] + '-' + String(parseInt(dateMatch[1])).padStart(2,'0') + '-' + String(parseInt(dateMatch[2])).padStart(2,'0');
  }

  // Find "Shooting Victims" row — columns are: 7-day(2026,2025,%chg), 28-day(2026,2025,%chg), YTD(2026,2025,%chg)
  // We need the YTD pair (5th and 6th numbers after the label)
  const shootMatch = text.match(/Shooting\s*Victims?\s+([\d,]+)\s+([\d,]+)\s+[-\d]+%\s+([\d,]+)\s+([\d,]+)\s+[-\d]+%\s+([\d,]+)\s+([\d,]+)/i);
  if (!shootMatch) throw new Error('StLouis: "Shooting Victims" not found. Tokens: ' + tokens.slice(0,60).join('|'));

  const ytd = parseInt(shootMatch[5].replace(/,/g, ''));
  const prior = parseInt(shootMatch[6].replace(/,/g, ''));
  console.log('StLouis: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}

// ─── Charlotte (ArcGIS monthly) ─────────────────────────────────────────────

async function fetchCharlotte() {
  const base = 'https://gis.charlottenc.gov/arcgis/rest/services/ODP/ViolentCrimeData/MapServer/0/query';
  const CURRENT_YEAR = new Date().getFullYear();

  const where = "ROW_TYPE = 'CMPD Jurisdiction' AND OFFENSE_DESCRIPTION = 'Non-Fatal Gunshot Injury'";
  const url = base + '?where=' + encodeURIComponent(where) +
    '&outFields=CALENDAR_YEAR,CALENDAR_MONTH,OFFENSE_COUNT&returnGeometry=false&resultRecordCount=2000&f=json';
  const resp = await fetchUrl(url);
  if (resp.status !== 200) throw new Error('Charlotte: HTTP ' + resp.status);
  const data = JSON.parse(resp.body.toString('utf8'));
  if (data.error) throw new Error('Charlotte: ' + (data.error.message || JSON.stringify(data.error).slice(0, 120)));
  const rows = data.features || [];

  // Find max month for current year
  let maxMonth = 0;
  rows.forEach(f => {
    const a = f.attributes;
    if (String(a.CALENDAR_YEAR) === String(CURRENT_YEAR) && parseInt(a.CALENDAR_MONTH) > maxMonth) {
      maxMonth = parseInt(a.CALENDAR_MONTH);
    }
  });

  function sumYear(year) {
    let total = 0;
    rows.forEach(f => {
      const a = f.attributes;
      if (String(a.CALENDAR_YEAR) === String(year) && parseInt(a.CALENDAR_MONTH) <= maxMonth) {
        total += parseInt(a.OFFENSE_COUNT) || 0;
      }
    });
    return total;
  }

  const ytd = sumYear(CURRENT_YEAR);
  const prior = sumYear(CURRENT_YEAR - 1);
  const asof = CURRENT_YEAR + '-' + String(maxMonth).padStart(2,'0') + '-28';

  console.log('Charlotte: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof + ' (through month ' + maxMonth + ')');
  return { ytd, prior, asof };
}



// ─── Main ─────────────────────────────────────────────────────────────────────



// ─── Omaha (REMOVED - Akamai CDN blocks all automated access) ────────────────






// ─── New Haven (CivicPlus CompStat PDF) ──────────────────────────────────────
// ─── Minneapolis (ArcGIS FeatureServer) ──────────────────────────────────────



async function fetchPhilly() {

  const CARTO = 'https://phl.carto.com/api/v2/sql?format=json&q=';
  const CURRENT_YEAR = new Date().getFullYear();

  // Latest date
  console.log('Philly: fetching latest date...');
  const latestSql = "SELECT date_ FROM shootings ORDER BY date_ DESC LIMIT 1";
  const latestResp = await fetchUrl(CARTO + encodeURIComponent(latestSql), 20000);
  if (latestResp.status !== 200) throw new Error('Philly latest: HTTP ' + latestResp.status);
  const latestData = JSON.parse(latestResp.body.toString('utf8'));
  if (!latestData.rows || !latestData.rows.length) throw new Error('Philly latest: no rows returned');
  const asof = String(latestData.rows[0].date_).slice(0, 10);
  const asofYear = parseInt(asof.slice(0, 4));

  // Build windows
  const ytdStart = asofYear + '-01-01';
  const priorStart = (asofYear - 1) + '-01-01';
  const priorEnd = (asofYear - 1) + asof.slice(4);

  async function fetchCount(startDate, endDate) {
    const sql = "SELECT COUNT(*) AS n FROM shootings WHERE date_ >= '" + startDate + "' AND date_ <= '" + endDate + "'";
    const resp = await fetchUrl(CARTO + encodeURIComponent(sql), 20000);
    if (resp.status !== 200) throw new Error('Philly count: HTTP ' + resp.status);
    const d = JSON.parse(resp.body.toString('utf8'));
    return parseInt(d.rows[0].n);
  }

  console.log('Philly: fetching YTD (' + ytdStart + ' to ' + asof + ') and prior (' + priorStart + ' to ' + priorEnd + ')...');
  const [ytd, prior] = await Promise.all([
    fetchCount(ytdStart, asof),
    fetchCount(priorStart, priorEnd),
  ]);

  console.log('Philly: asof=' + asof + ' ytd=' + ytd + ' prior=' + prior);
  return { ytd, prior, asof };

}



async function fetchMinneapolis() {

  const BASE = 'https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Crime_Data/FeatureServer/0/query';

  const CURRENT_YEAR = new Date().getFullYear();



  function buildStatUrl(startDate, endDate) {

    const where = "Type = 'Gunshot Wound Victims'" +

      " AND Reported_Date >= TIMESTAMP '" + startDate + " 00:00:00'" +

      " AND Reported_Date <= TIMESTAMP '" + endDate + " 23:59:59'";

    const stats = JSON.stringify([{ statisticType: 'sum', onStatisticField: 'Crime_Count', outStatisticFieldName: 'total' }]);

    return BASE + '?where=' + encodeURIComponent(where) + '&outStatistics=' + encodeURIComponent(stats) + '&f=json';

  }



  async function fetchLatest() {

    const where = "Type = 'Gunshot Wound Victims'";

    const url = BASE + '?where=' + encodeURIComponent(where) +

      '&outFields=Reported_Date&orderByFields=Reported_Date+DESC&resultRecordCount=1&f=json';

    const resp = await fetchUrl(url, 20000);

    if (resp.status !== 200) throw new Error('Minneapolis latest: HTTP ' + resp.status);

    const d = JSON.parse(resp.body.toString('utf8'));

    if (d.error) throw new Error('Minneapolis latest ArcGIS error: ' + (d.error.message || JSON.stringify(d.error).slice(0, 80)));

    if (!d.features || !d.features.length) throw new Error('Minneapolis latest: no features returned');

    const raw = d.features[0].attributes.Reported_Date;

    if (typeof raw === 'number') {

      const dt = new Date(raw);

      const mm = String(dt.getMonth() + 1).padStart(2, '0');

      const dd = String(dt.getDate()).padStart(2, '0');

      return dt.getFullYear() + '-' + mm + '-' + dd;

    }

    return String(raw).slice(0, 10).replace(/\//g, '-');

  }



  async function fetchSum(startDate, endDate) {

    const url = buildStatUrl(startDate, endDate);

    const resp = await fetchUrl(url, 20000);

    if (resp.status !== 200) throw new Error('Minneapolis count: HTTP ' + resp.status);

    const d = JSON.parse(resp.body.toString('utf8'));

    if (d.error) throw new Error('Minneapolis ArcGIS error: ' + (d.error.message || JSON.stringify(d.error).slice(0, 80)));

    if (!d.features || !d.features.length) return 0;

    return d.features[0].attributes.total || 0;

  }



  console.log('Minneapolis: fetching latest date...');

  const asof = await fetchLatest();

  const asofYear = parseInt(asof.slice(0, 4));



  const ytdStart = asofYear + '-01-01';

  const priorStart = (asofYear - 1) + '-01-01';

  const priorEnd = (asofYear - 1) + asof.slice(4);



  console.log('Minneapolis: fetching YTD (' + ytdStart + ' to ' + asof + ') and prior (' + priorStart + ' to ' + priorEnd + ')...');

  const [ytd, prior] = await Promise.all([

    fetchSum(ytdStart, asof),

    fetchSum(priorStart, priorEnd),

  ]);



  console.log('Minneapolis: asof=' + asof + ' ytd=' + ytd + ' prior=' + prior);

  return { ytd, prior, asof };

}








async function main() {

  const fetchedAt = new Date().toISOString();

  const outDir = path.join(__dirname, '..', 'data');

  const outPath = path.join(outDir, 'manual-auto.json');



  let existing = {};

  try { existing = JSON.parse(fs.readFileSync(outPath, 'utf8')); } catch (e) { /* first run */ }



  const results = {};



  function safe(name, fn, timeoutMs) {

    timeoutMs = timeoutMs || 120000;

    const timer = new Promise((_, reject) => setTimeout(() => reject(new Error(name + ' timed out after ' + (timeoutMs/1000) + 's')), timeoutMs));

    return Promise.race([fn(), timer])

      .then(function(r) {

        console.log('\n--- ' + name + ' OK ---');

        console.log(name + ':', { ...r, fetchedAt, ok: true });

        return { key: name.toLowerCase().replace(/[^a-z]/g,''), value: { ...r, fetchedAt, ok: true } };

      })

      .catch(function(e) {

        console.error('\n--- ' + name + ' FAILED:', e.message, '---');

        return { key: name.toLowerCase().replace(/[^a-z]/g,''), value: { ok: false, error: e.message, fetchedAt } };

      });

  }



  console.log('Starting all fetches in parallel...');

  const fetches = await Promise.all([

    safe('Philly',      fetchPhilly,      60000),

    safe('Minneapolis', fetchMinneapolis, 60000),

    safe('Detroit',    fetchDetroit,    120000),

    safe('Durham',     fetchDurham,     60000),

    safe('Milwaukee',  fetchMilwaukee,  60000),

    safe('Memphis',    fetchMemphis,    120000),

    safe('MiamiDade',  fetchMiamiDade,  120000),

    safe('Pittsburgh', fetchPittsburgh, 120000),

    safe('Portland',   fetchPortland,   60000),

    safe('Buffalo',    fetchBuffalo,    180000),

    safe('Albany',     fetchAlbany,     180000),

    safe('Syracuse',   fetchSyracuse,   180000),

    safe('SuffolkCounty', fetchSuffolkCounty, 180000),

    safe('Nashville',  fetchNashville,  180000),

    safe('Hartford',   fetchHartford,   60000),

    safe('Denver',     fetchDenver,     120000),

    safe('Portsmouth',  fetchPortsmouth,  120000),

    safe('Wilmington',  fetchWilmington,  120000),

    safe('LasVegas',   fetchVegas,      120000),

    safe('Chicago',    fetchChicago,    180000),
    safe('NYC',        fetchNYC,        60000),
    safe('Baltimore',  fetchBaltimore,  120000),
    safe('Boston',     fetchBoston,     60000),
    safe('Louisville', fetchLouisville, 60000),
    safe('Seattle',    fetchSeattle,    120000),
    safe('Cincinnati', fetchCincinnati, 120000),
    safe('StLouis',    fetchStLouis,    60000),
    safe('NewOrleans', fetchNewOrleans, 120000),
    safe('Charlotte',  fetchCharlotte,  60000),
    safe('Rochester',  fetchRochester,  60000),

    // Omaha removed — Akamai blocks all automated access
  ]);



  for (const { key, value } of fetches) {

    if (value.ok) {

      if (!value.asof && existing[key] && existing[key].asof) {
        value.asof = existing[key].asof;
      }
      results[key] = value;

    } else if (existing[key] && existing[key].ok) {

      console.log(key + ': keeping previous good data (ytd=' + existing[key].ytd + ' asof=' + existing[key].asof + ')');

      results[key] = existing[key];

      results[key].stale = true;

    } else {

      results[key] = value;

    }

  }



  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));

  console.log('\nWrote', outPath);

  console.log(JSON.stringify(results, null, 2));

  // ── Append today's aggregate to trend.json ──
  const trendPath = path.join(outDir, 'trend.json');
  let trend = [];
  try { trend = JSON.parse(fs.readFileSync(trendPath, 'utf8')); } catch(e) {}

  let totalYtd = 0, totalPrior = 0, cityCount = 0;
  for (const [, d] of Object.entries(results)) {
    if (d.ok && typeof d.ytd === 'number' && typeof d.prior === 'number' && d.prior > 0) {
      totalYtd += d.ytd;
      totalPrior += d.prior;
      cityCount++;
    }
  }
  const today = new Date().toISOString().slice(0, 10);
  const pctChange = totalPrior > 0 ? Math.round(((totalYtd - totalPrior) / totalPrior * 100) * 10) / 10 : null;

  // Replace today's entry if it exists, otherwise append
  const idx = trend.findIndex(t => t.date === today);
  const entry = { date: today, ytd: totalYtd, prior: totalPrior, cities: cityCount, pct: pctChange };
  if (idx >= 0) trend[idx] = entry; else trend.push(entry);
  trend.sort((a, b) => a.date.localeCompare(b.date));

  fs.writeFileSync(trendPath, JSON.stringify(trend, null, 2));
  console.log('\nTrend: ' + today + ' ytd=' + totalYtd + ' prior=' + totalPrior + ' n=' + cityCount + ' pct=' + pctChange + '% (' + trend.length + ' total days)');

}



async function runSelectedCity() {
  const cityArgIndex = process.argv.indexOf('--city');
  if (cityArgIndex < 0) return false;

  const city = String(process.argv[cityArgIndex + 1] || '').toLowerCase().replace(/[^a-z]/g, '');
  const fetchers = {
    baltimore: fetchBaltimore,
    chicago: fetchChicago,
    cincinnati: fetchCincinnati,
    lasvegas: fetchVegas,
    memphis: fetchMemphis,
    miamidade: fetchMiamiDade,
    neworleans: fetchNewOrleans,
    portsmouth: fetchPortsmouth,
    seattle: fetchSeattle,
    denver: fetchDenver,
    stlouis: fetchStLouis
  };
  if (!fetchers[city]) throw new Error('Unknown --city value: ' + (process.argv[cityArgIndex + 1] || ''));

  const result = await fetchers[city]();
  console.log(JSON.stringify({ [city]: { ...result, ok: true } }, null, 2));
  return true;
}

runSelectedCity()
  .then(handled => { if (!handled) return main(); })
  .catch(e => { console.error(e); process.exit(1); });





// ─── Portland (CSV from Tableau Public) ──────────────────────────────────────



async function fetchPortland() {

  const csvUrl = 'https://public.tableau.com/views/PPBOpenDataDownloads/Shootings.csv?:showVizHome=no';

  console.log('Portland: fetching CSV...');

  const resp = await fetchUrl(csvUrl, 30000);

  if (resp.status !== 200) throw new Error('Portland: HTTP ' + resp.status);



  const text = resp.body.toString('utf8');

  const lines = text.split('\n');

  console.log('Portland: CSV lines:', lines.length);



  const header = parseCsvLine(lines[0]);

  const iYear = header.indexOf('Occur Year');

  const iMonth = header.indexOf('Occur Month');

  const iType = header.indexOf('Shooting Type');

  console.log('Portland: columns - Year:', iYear, 'Month:', iMonth, 'Type:', iType);



  if (iYear < 0 || iMonth < 0 || iType < 0) {

    throw new Error('Portland: CSV columns not found. Header: ' + header.join(', '));

  }



  const yr = new Date().getFullYear();



  const rows = [];

  for (let i = 1; i < lines.length; i++) {

    if (!lines[i].trim()) continue;

    const cols = parseCsvLine(lines[i]);

    const type = cols[iType];

    if (type === 'No Injury') continue;

    const year = parseInt(cols[iYear]);

    const month = parseInt(cols[iMonth]);

    if (!year || !month) continue;

    rows.push({ year, month });

  }

  console.log('Portland: qualifying rows (excl No Injury):', rows.length);



  let maxMonth = 0;

  rows.forEach(r => { if (r.year === yr && r.month > maxMonth) maxMonth = r.month; });

  // If no current year data, use max month from prior year for fair YTD comparison
  if (maxMonth === 0) {
    rows.forEach(r => { if (r.year === yr - 1 && r.month > maxMonth) maxMonth = r.month; });
    console.log('Portland: no ' + yr + ' data, using prior year max month:', maxMonth);
  } else {
    console.log('Portland: max month in ' + yr + ':', maxMonth);
  }



  let ytd = 0, prior = 0;

  rows.forEach(r => {

    if (maxMonth > 0 && r.month > maxMonth) return;

    if (r.year === yr) ytd++;

    if (r.year === yr - 1) prior++;

  });



  let asof = null;

  if (maxMonth > 0) {

    const lastDay = new Date(yr, maxMonth, 0).getDate();

    asof = yr + '-' + String(maxMonth).padStart(2, '0') + '-' + String(lastDay).padStart(2, '0');

  }



  console.log('Portland final: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);

  if (ytd === 0 && prior === 0) throw new Error('Portland: parsed all zeros');

  return { ytd, prior, asof };

}





// ─── Denver (Power BI API - Firearm Homicides + Non-Fatal Shootings) ────────

async function fetchDenver() {
  console.log('Denver: querying Power BI API (YTD measures)...');
  const yr = new Date().getFullYear();

  // Denver has explicit YTD measures that compare same-period across years
  const dsr = await pbiQuery(
    'wabi-us-gov-iowa-api.analysis.usgovcloudapi.net',
    '9c0f840f-824d-4dec-8f61-311d278e3c42',
    713694, 'f869e8c9-a501-45a6-a4c2-d6eae79af2ed',
    {
      Version: 2,
      From: [
        { Name: 's', Entity: '2021-2026 Shooting', Type: 0 },
        { Name: 'h', Entity: '2021-2026 Homicides', Type: 0 }
      ],
      Select: [
        { Measure: { Expression: { SourceRef: { Source: 's' } }, Property: yr + 'YTD_NFS' }, Name: 'NFS_YTD' },
        { Measure: { Expression: { SourceRef: { Source: 's' } }, Property: (yr-1) + 'YTD_NFS' }, Name: 'NFS_Prior' },
        { Measure: { Expression: { SourceRef: { Source: 'h' } }, Property: yr + 'YTD_Hom' }, Name: 'Hom_YTD' },
        { Measure: { Expression: { SourceRef: { Source: 'h' } }, Property: (yr-1) + 'YTD_Hom' }, Name: 'Hom_Prior' }
      ]
    }
  );

  // Scalar response: DM0[0].C = [nfsYtd, nfsPrior, homYtd, homPrior]
  const vals = dsr.DS[0].PH[0].DM0[0].C;
  if (!vals || vals.length < 4) throw new Error('Denver: unexpected PBI response');

  const nfsYtd = vals[0] || 0, nfsPrior = vals[1] || 0;
  const homYtd = vals[2] || 0, homPrior = vals[3] || 0;
  const ytd = nfsYtd + homYtd;
  const prior = nfsPrior + homPrior;

  const asof = await pbiAsOf(
    'wabi-us-gov-iowa-api.analysis.usgovcloudapi.net',
    '9c0f840f-824d-4dec-8f61-311d278e3c42',
    713694, 'f869e8c9-a501-45a6-a4c2-d6eae79af2ed',
    {
      Version: 2,
      From: [{ Name: 's', Entity: '2021-2026 Shooting', Type: 0 }],
      Select: [
        { Aggregation: { Expression: { Column: { Expression: { SourceRef: { Source: 's' } }, Property: 'OCC Date' } }, Function: 4 }, Name: 'MaxOccDate' }
      ]
    },
    'Denver'
  );
  console.log('Denver: NFS=' + nfsYtd + '/' + nfsPrior + ' Hom=' + homYtd + '/' + homPrior + ' ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}


// ─── Portsmouth (Power BI - GSW Victims) ─────────────────────────────────────



async function fetchPortsmouth() {
  const { chromium } = require('playwright');
  const yr = new Date().getFullYear();
  const cluster = 'wabi-us-gov-virginia-api.analysis.usgovcloudapi.net';
  const reportKey = 'd77fd2c3-982b-4843-98ee-ed2cfd839ecd';
  const modelId = 1497723;
  const datasetId = 'e8e96817-5c6b-4691-8745-4ffaf7d3a39b';
  const reportId = 'f774ff90-8529-4a94-8ee2-3bb97cce137a';
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 900 });
  page.setDefaultTimeout(30000);

  const url = 'https://app.powerbigov.us/view?r=eyJrIjoiZDc3ZmQyYzMtOTgyYi00ODQzLTk4ZWUtZWQyY2ZkODM5ZWNkIiwidCI6ImM3N2RiNGQ4LWEwZjUtNDU0YS05MmMxLWI3ZDg0YzY0ZmQ0NCJ9';
  console.log('Portsmouth: loading Power BI dashboard...');
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await waitForPowerBI(page, 15000);
  await page.waitForFunction(
    () => /Last\s+(?:Database\s+)?Update[d]?[\s\S]*\d{1,2}\/\d{1,2}\/\d{4}/i.test(document.body.innerText),
    null,
    { timeout: 20000 }
  ).catch(() => {});

  const bodyText = await page.evaluate(() => document.body.innerText);
  console.log('Portsmouth page sample:', bodyText.substring(0, 800));

  let asof = null;
  const dateMatch = bodyText.match(/(?:Last\s+(?:Database\s+)?Update[d]?|Updated)[\s\S]{0,80}?(\d{1,2})\/(\d{1,2})\/(\d{4})/i) ||
    bodyText.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }
  await browser.close();

  const dsr = await pbiDataShapeQuery(
    cluster,
    reportKey,
    modelId,
    datasetId,
    {
      Query: {
        Version: 2,
        From: [{ Name: 'c', Entity: 'Crimes', Type: 0 }],
        Select: [
          { HierarchyLevel: { Expression: { Hierarchy: { Expression: { PropertyVariationSource: { Expression: { SourceRef: { Source: 'c' } }, Name: 'Variation', Property: 'Date Occurred' } }, Hierarchy: 'Date Hierarchy' } }, Level: 'Year' }, Name: 'Crimes.date_occu.Variation.Date Hierarchy.Year', NativeReferenceName: 'date_occu Year' },
          { Aggregation: { Expression: { Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'GSWs' } }, Function: 0 }, Name: 'Sum(Crimes.GSWs)', NativeReferenceName: 'Total Gunshot Victims' },
          { Measure: { Expression: { SourceRef: { Source: 'c' } }, Property: 'GSW Non-Fatal' }, Name: 'Crimes.GSW Non-Fatal', NativeReferenceName: 'Non-Fatal' },
          { Measure: { Expression: { SourceRef: { Source: 'c' } }, Property: 'FatalityPercent' }, Name: 'Crimes.FatalityPercent', NativeReferenceName: 'Fatality %' },
          { Aggregation: { Expression: { Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'Suicide Deaths' } }, Function: 0 }, Name: 'Sum(Crimes.Suicide Deaths)', NativeReferenceName: 'Fatal (Suicide)' },
          { Aggregation: { Expression: { Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'Non-Suicide Deaths' } }, Function: 0 }, Name: 'Sum(Crimes.Non-Suicide Deaths)', NativeReferenceName: 'Fatal (Non-Suicide)' }
        ],
        Where: [
          { Condition: { Comparison: { ComparisonKind: 0, Left: { Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'chrgcnt' } }, Right: { Literal: { Value: '1L' } } } } },
          { Condition: { In: { Expressions: [{ Column: { Expression: { SourceRef: { Source: 'c' } }, Property: 'YTDFlag' } }], Values: [[{ Literal: { Value: 'true' } }]] } } }
        ]
      },
      Binding: {
        Primary: { Groupings: [{ Projections: [0, 1, 2, 3, 4, 5] }] },
        DataReduction: { DataVolume: 4, Primary: { Sample: {} } },
        Version: 1
      },
      ExecutionMetricsKind: 1
    },
    [{ ReportId: reportId, VisualId: 'ccd3c794b5fbb8cbc018' }]
  );

  const rows = dsr?.DS?.[0]?.PH?.[0]?.DM0 || [];
  const byYear = {};
  for (const row of rows) {
    const c = row.C || [];
    byYear[c[0]] = { total: c[1], nonFatal: c[2], suicide: c[4], nonSuicide: c[5] };
  }

  const cur = byYear[yr];
  const prev = byYear[yr - 1];
  if (!cur || !prev) throw new Error('Portsmouth: missing YTD rows. Years: ' + Object.keys(byYear).join(', '));

  const ytd = cur.total - cur.suicide;
  const prior = prev.total - prev.suicide;

  console.log('Portsmouth parsed: ' + yr + ' total=' + cur.total + ' suicide=' + cur.suicide +
    ' | ' + (yr-1) + ' total=' + prev.total + ' suicide=' + prev.suicide);
  console.log('Portsmouth final: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);

  return { ytd, prior, asof };
}





// ─── Hartford (CompStat PDF) ─────────────────────────────────────────────────



async function fetchHartford() {

  function getWeekEndingSaturdays() {

    const dates = [];

    const now = new Date();

    for (let i = 0; i < 8; i++) {

      const d = new Date(now);

      d.setDate(d.getDate() - d.getDay() - 1 - (7 * i));

      dates.push(d);

    }

    return dates;

  }



  function buildUrl(d) {

    const yyyy = d.getFullYear();

    const mm = String(d.getMonth() + 1).padStart(2, '0');

    const dd = String(d.getDate()).padStart(2, '0');

    const yy = String(yyyy).slice(-2);

    return 'https://www.hartfordct.gov/files/assets/public/v/1/police/police-documents/compstat/' + yyyy + '/' + mm + '/we-' + mm + '-' + dd + '-' + yy + '.pdf';

  }



  function fmtDate(d) {

    return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');

  }



  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const context = await browser.newContext({ acceptDownloads: true });

  const page = await context.newPage();



  const saturdays = getWeekEndingSaturdays();

  let pdfBuffer = null;

  let asof = null;



  try {

    for (const d of saturdays) {

      const url = buildUrl(d);

      console.log('Hartford: trying', url);

      try {

        const probe = await context.request.fetch(url, { method: 'HEAD', timeout: 10000 }).catch(() => null);

        const probeStatus = probe ? probe.status() : 0;

        console.log('Hartford:   status=' + probeStatus);

        if (probeStatus !== 200) continue;



        const [download] = await Promise.all([

          page.waitForEvent('download', { timeout: 30000 }),

          page.goto(url, { waitUntil: 'commit', timeout: 30000 }).catch(() => {})

        ]);

        const downloadPath = await download.path();

        if (!downloadPath) { console.log('Hartford:   download path null'); continue; }

        const body = require('fs').readFileSync(downloadPath);

        if (body.length > 10000 && body[0] === 0x25) {

          pdfBuffer = body;

          asof = fmtDate(d);

          console.log('Hartford: downloaded PDF for', asof, '(' + (body.length / 1024).toFixed(0) + ' KB)');

          break;

        }

      } catch(e) {

        console.log('Hartford:   error:', e.message);

      }

    }

  } finally {

    await browser.close();

  }



  if (!pdfBuffer) throw new Error('Hartford: could not download any recent CompStat PDF');



  const tokens = await extractPdfTokens(pdfBuffer, 2);

  const joined = tokens.join(' ');



  function parseVictimRow(label) {

    var idx = joined.indexOf(label);

    if (idx === -1) return { ytd2026: 0, ytd2025: 0 };

    var afterLabel = joined.substring(idx + label.length).trim();

    var vals = afterLabel.split(/\s+/).slice(0, 13);

    function parseVal(s) {

      if (!s || s === '-') return 0;

      var n = parseInt(s.replace(/,/g, ''));

      return isNaN(n) ? 0 : n;

    }

    return { ytd: parseVal(vals[8]), prior: parseVal(vals[9]) };

  }



  var nonfatal = parseVictimRow('Non_Fatal Shooting Victims');



  console.log('Hartford: non-fatal YTD=' + nonfatal.ytd + ' prior=' + nonfatal.prior);



  var ytd = nonfatal.ytd;

  var prior = nonfatal.prior;



  var ytdMatch = joined.match(/Year\s+to\s+Date.*?to\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);

  if (ytdMatch) {

    var months = {jan:1,january:1,feb:2,february:2,mar:3,march:3,apr:4,april:4,may:5,jun:6,june:6,jul:7,july:7,aug:8,august:8,sep:9,september:9,oct:10,october:10,nov:11,november:11,dec:12,december:12};

    var mo = months[ytdMatch[1].toLowerCase()];

    if (mo) asof = ytdMatch[3] + '-' + String(mo).padStart(2,'0') + '-' + String(parseInt(ytdMatch[2])).padStart(2,'0');

  }



  return { ytd: ytd, prior: prior, asof: asof };

}





// ─── Wilmington (WPD CompStat PDF) ──────────────────────────────────────────

async function fetchWilmington() {
  const { chromium } = require('playwright');
  // Site blocks headless browsers (403), so launch headed with anti-detection.
  // GitHub Actions uses Xvfb to provide a virtual display.
  const browser = await chromium.launch({
    headless: false,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox']
  });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
  });
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  const page = await context.newPage();

  try {
    // Visit the CompStat page to get session cookies (direct PDF fetch returns 403)
    const listUrl = 'https://www.wilmingtonde.gov/government/public-safety/wilmington-police-department/compstat-reports';
    console.log('Wilmington: loading CompStat page...');
    await page.goto(listUrl, { waitUntil: 'networkidle', timeout: 60000 });
    await page.waitForTimeout(5000);

    // Find the PDF link — text always starts with "WPD CompStat Report"
    const linkInfo = await page.evaluate(() => {
      const links = Array.from(document.querySelectorAll('a'));
      const link = links.find(a => a.textContent.trim().startsWith('WPD CompStat Report'));
      return link ? { href: link.href, text: link.textContent.trim() } : null;
    });
    if (!linkInfo) throw new Error('Wilmington: could not find CompStat PDF link on page');
    var pdfUrl = linkInfo.href;
    console.log('Wilmington: PDF link:', linkInfo.text);
    console.log('Wilmington: PDF URL:', pdfUrl);

    // Download PDF via in-page fetch (uses session cookies)
    const base64 = await page.evaluate(async (url) => {
      const r = await fetch(url);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const buf = await r.arrayBuffer();
      const bytes = new Uint8Array(buf);
      let binary = '';
      for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
      return btoa(binary);
    }, pdfUrl);

    const pdfBuffer = Buffer.from(base64, 'base64');
    console.log('Wilmington: PDF downloaded (' + (pdfBuffer.length / 1024).toFixed(0) + ' KB)');

    // Parse page 1 (Citywide)
    const tokens = await extractPdfTokens(pdfBuffer, 1);
    var joined = tokens.join(' ');

    // Extract as-of date from the link text, e.g. "WPD CompStat Report - April 13 through April 19, 2026"
    // This is more reliable than parsing fragmented PDF tokens
    var asof = null;
    var months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
    var linkDateMatch = linkInfo.text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
    if (linkDateMatch) {
      var mo = months[linkDateMatch[1].toLowerCase()];
      if (mo) asof = linkDateMatch[3] + '-' + String(mo).padStart(2, '0') + '-' + String(parseInt(linkDateMatch[2])).padStart(2, '0');
    }

    // Find "Shooting Victims" row and extract YTD columns
    // PDF layout per row: val val %chg val val %chg val val %chg ...
    // Groups of 3: LAST 7 DAYS (2026, 2025, %CHG) | LAST 28 DAYS | YEAR TO DATE
    // YTD 2026 = group 3, position 1 = overall index 6; YTD 2025 = index 7
    var svMatch = joined.match(/Shooting\s+Victims\s+([\s\S]*?)(?:\*?Juv|Theft|$)/i);
    if (!svMatch) throw new Error('Wilmington: could not find "Shooting Victims" row in PDF. Sample: ' + tokens.slice(0, 30).join('|'));

    var afterSV = svMatch[1];
    var numTokens = afterSV.match(/(\*|-?\d+%?)/g) || [];
    var nums = numTokens.map(function(t) {
      if (t === '*') return 0;
      return parseInt(t.replace(/%$/, ''));
    });

    // YTD 2026 is at index 6, YTD 2025 at index 7
    var ytd = nums[6];
    var prior = nums[7];

    console.log('Wilmington: YTD=' + ytd + ' prior=' + prior + ' asof=' + asof);

    if (ytd == null || isNaN(ytd)) throw new Error('Wilmington: failed to parse YTD from PDF. nums=' + JSON.stringify(nums));

    return { ytd, prior, asof };
  } finally {
    await browser.close();
  }
}


// ─── Nashville (MNPD Crime Initiative Book PDF) ─────────────────────────────



async function fetchNashville() {



  function getReportDatesToTry() {

    const dates = [];

    const now = new Date();

    for (let weeksBack = 0; weeksBack <= 4; weeksBack++) {

      const sat = new Date(now);

      sat.setDate(sat.getDate() - sat.getDay() - 1 - (7 * weeksBack));

      dates.push(formatDateStr(sat));

      const fri = new Date(sat);

      fri.setDate(fri.getDate() - 1);

      dates.push(formatDateStr(fri));

    }

    return [...new Set(dates)];

  }



  function formatDateStr(d) {

    return d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

  }



  const downloadDir = path.join(__dirname, '..', 'data', 'nashville-downloads');

  if (!fs.existsSync(downloadDir)) fs.mkdirSync(downloadDir, { recursive: true });



  async function downloadPdf(dateStr) {

    const year = dateStr.substring(0, 4);

    const filename = `${dateStr}_Crime_Initiative_Book.pdf`;

    const localPath = path.join(downloadDir, filename);



    if (fs.existsSync(localPath) && fs.statSync(localPath).size > 100000) {

      console.log('Nashville: using cached PDF:', filename);

      return localPath;

    }



    const directUrls = [

      `https://metronashville.sharepoint.com/sites/MNPDCrimeAnalysis-Public/Shared%20Documents/Weekly%20Crime%20-%20Initiative%20Book/${year}/${filename}`,

      `https://metronashville.sharepoint.com/sites/MNPDCrimeAnalysis-Public/_layouts/15/download.aspx?SourceUrl=/sites/MNPDCrimeAnalysis-Public/Shared%20Documents/Weekly%20Crime%20-%20Initiative%20Book/${year}/${filename}`,

    ];



    for (const url of directUrls) {

      try {

        console.log('Nashville: trying direct URL for', dateStr, '...');

        const resp = await fetchUrl(url, 30000);

        if (resp.status === 200 && resp.body.length > 100000 && resp.body[0] === 0x25 && resp.body[1] === 0x50) {

          fs.writeFileSync(localPath, resp.body);

          console.log('Nashville: downloaded via direct URL (' + (resp.body.length / 1024 / 1024).toFixed(1) + ' MB)');

          return localPath;

        }

      } catch (e) { /* try next */ }

    }



    try {

      console.log('Nashville: trying Playwright for', dateStr, '...');

      const { chromium } = require('playwright');

      const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });

      const page = await browser.newPage();

      await page.setViewportSize({ width: 1920, height: 1080 });



      const spUrl = directUrls[0];

      const response = await page.goto(spUrl, { waitUntil: 'networkidle', timeout: 45000 }).catch(() => null);



      if (response) {

        const contentType = response.headers()['content-type'] || '';

        if (contentType.includes('pdf')) {

          const buffer = await response.body().catch(() => null);

          if (buffer && buffer.length > 100000) {

            fs.writeFileSync(localPath, buffer);

            await browser.close();

            console.log('Nashville: downloaded via Playwright direct (' + (buffer.length / 1024 / 1024).toFixed(1) + ' MB)');

            return localPath;

          }

        }

      }



      const shareLink = 'https://metronashville.sharepoint.com/:f:/s/MNPDCrimeAnalysis-Public/Ei-WvJMw8N5OiETXZcnTwlgBlnNytrIMj_wiYADfzMln9g?e=L5g6b2';

      console.log('Nashville: navigating SharePoint folder UI...');

      await page.goto(shareLink, { waitUntil: 'networkidle', timeout: 60000 }).catch(() => {});

      await page.waitForTimeout(5000);



      const yearEl = await page.locator(`text=${year}`).first();

      if (await yearEl.isVisible({ timeout: 5000 }).catch(() => false)) {

        await yearEl.click();

        await page.waitForTimeout(5000);

      }



      const fileEl = await page.locator(`text=${dateStr}`).first();

      if (await fileEl.isVisible({ timeout: 5000 }).catch(() => false)) {

        await fileEl.click();

        await page.waitForTimeout(3000);



        for (const sel of ['[data-automationid="downloadCommand"]', '[aria-label*="Download"]', 'button:has-text("Download")']) {

          try {

            const btn = await page.locator(sel).first();

            if (await btn.isVisible({ timeout: 3000 }).catch(() => false)) {

              const [download] = await Promise.all([

                page.waitForEvent('download', { timeout: 30000 }),

                btn.click()

              ]);

              const stream = await download.createReadStream();

              const chunks = [];

              await new Promise((res, rej) => {

                stream.on('data', c => chunks.push(c));

                stream.on('end', res);

                stream.on('error', rej);

              });

              const buf = Buffer.concat(chunks);

              if (buf.length > 100000) {

                fs.writeFileSync(localPath, buf);

                await browser.close();

                console.log('Nashville: downloaded via SharePoint UI (' + (buf.length / 1024 / 1024).toFixed(1) + ' MB)');

                return localPath;

              }

              break;

            }

          } catch (e) { /* try next selector */ }

        }

      }



      await browser.close();

    } catch (e) {

      console.log('Nashville: Playwright strategy failed:', e.message);

    }



    return null;

  }



  let pdfPath = null;



  // Always try downloading the latest PDF first

  const datesToTry = getReportDatesToTry();

  console.log('Nashville: trying dates:', datesToTry.slice(0, 6).join(', '));

  for (const dateStr of datesToTry) {

    pdfPath = await downloadPdf(dateStr);

    if (pdfPath) break;

  }



  // Fall back to most recent local PDF if download failed

  if (!pdfPath && fs.existsSync(downloadDir)) {

    const existing = fs.readdirSync(downloadDir)

      .filter(f => f.endsWith('.pdf') && f.includes('Crime_Initiative_Book'))

      .sort().reverse();

    if (existing.length > 0) {

      pdfPath = path.join(downloadDir, existing[0]);

      console.log('Nashville: falling back to local PDF:', existing[0]);

    }

  }



  if (!pdfPath) {

    throw new Error('Nashville: could not obtain Crime Initiative Book PDF. Place it manually in data/nashville-downloads/');

  }



  console.log('Nashville: parsing', path.basename(pdfPath));

  const pdfBuffer = fs.readFileSync(pdfPath);

  let pdfjsLib;

  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }

  catch(e) { pdfjsLib = require(path.join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }

  pdfjsLib.GlobalWorkerOptions.workerSrc = false;

  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfBuffer) }).promise;

  console.log('Nashville: PDF has', pdf.numPages, 'pages');



  const pages = [];

  for (let i = 1; i <= pdf.numPages; i++) {

    const pg = await pdf.getPage(i);

    const tc = await pg.getTextContent();

    let lastY = null;

    let text = '';

    for (const item of tc.items) {

      const y = Math.round(item.transform[5]);

      if (lastY !== null && Math.abs(y - lastY) > 2) {

        text += '\n';

      } else if (lastY !== null) {

        text += ' ';

      }

      text += item.str;

      lastY = y;

    }

    pages.push(text);

  }



  const targetIdx = findGunShotVictimsPage(pages);

  if (targetIdx === -1) {

    throw new Error('Nashville: could not find "Gunshot Victims" page in PDF');

  }

  console.log('Nashville: found Gunshot Victims page at page', targetIdx + 1);



  const pageText = pages[targetIdx];

  const parsed = parseGunShotVictimsPage(pageText);



  let asof = null;

  const dateMatch = path.basename(pdfPath).match(/(\d{8})/);

  if (dateMatch) {

    const d = dateMatch[1];

    asof = d.substring(0, 4) + '-' + d.substring(4, 6) + '-' + d.substring(6, 8);

  }



  console.log('Nashville: fatal=' + parsed.fatal.current + ' (prior=' + parsed.fatal.prior + ')');

  console.log('Nashville: nonFatal=' + parsed.nonFatal.current + ' (prior=' + parsed.nonFatal.prior + ')');



  const ytd = parsed.fatal.current + parsed.nonFatal.current;

  const prior = parsed.fatal.prior + parsed.nonFatal.prior;



  console.log('Nashville: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);

  if (ytd === 0 && prior === 0) throw new Error('Nashville: parsed all zeros');



  return { ytd, prior, asof };

}



function findGunShotVictimsPage(pages) {

  const expected = 145;

  const searchOrder = [expected];

  for (let offset = 1; offset <= 15; offset++) {

    searchOrder.push(expected + offset);

    searchOrder.push(expected - offset);

  }

  for (let i = 0; i < pages.length; i++) {

    if (!searchOrder.includes(i)) searchOrder.push(i);

  }



  for (const idx of searchOrder) {

    if (idx < 0 || idx >= pages.length) continue;

    const text = (pages[idx] || '').toUpperCase();

    if (text.includes('GUNSHOT VICTIMS') &&

        text.includes('COUNTY') &&

        text.includes('GUNSHOT HOMICIDE') &&

        text.includes('GUNSHOT INJURY')) {

      return idx;

    }

  }

  return -1;

}



function parseGunShotVictimsPage(pageText) {

  const lines = pageText.split('\n').map(function(l) { return l.trim(); }).filter(function(l) { return l.length > 0; });



  const result = {

    fatal:    { current: null, prior: null, change: null },

    nonFatal: { current: null, prior: null, change: null },

    propertyDamage: { current: null, prior: null, change: null },

  };



  let countyStart = -1;

  for (let i = 0; i < lines.length; i++) {

    if (/\bCounty\b/i.test(lines[i])) { countyStart = i; break; }

  }

  if (countyStart === -1) {

    console.log('Nashville: WARNING - County row not found');

    return result;

  }



  for (let i = countyStart; i < Math.min(countyStart + 8, lines.length); i++) {

    const line = lines[i];

    const upper = line.toUpperCase();

    if (/^(Information summarized|Sourced from)/i.test(line)) break;



    const nums = nashvilleExtractNumbers(line);

    const groups = nashvilleFindValidGroups(nums);



    if (upper.includes('GUNSHOT HOMICIDE') && groups.length >= 3) {

      result.fatal = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };

    } else if (upper.includes('GUNSHOT INJURY') && !upper.includes('HOMICIDE') && groups.length >= 3) {

      result.nonFatal = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };

    } else if (upper.includes('PROPERTY DAMAGE') && groups.length >= 3) {

      result.propertyDamage = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };

    }

  }



  return result;

}



function nashvilleExtractNumbers(text) {

  var results = [];

  var regex = /-?\d[\d,]*\.?\d*/g;

  var match;

  while ((match = regex.exec(text)) !== null) {

    var val = parseFloat(match[0].replace(/,/g, ''));

    if (!isNaN(val)) results.push(val);

  }

  return results;

}



function nashvilleFindValidGroups(nums) {

  var groups = [];

  var i = 0;

  while (i <= nums.length - 3) {

    var v1 = nums[i], v2 = nums[i + 1], v3 = nums[i + 2];

    if (v3 === v2 - v1) {

      groups.push({ prior: v1, current: v2, change: v3 });

      i += 3;

    } else {

      i++;

    }

  }

  return groups;

}

