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





// ─── Memphis (Power BI) ───────────────────────────────────────────────────────



async function fetchMemphis() {

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  page.setDefaultTimeout(30000);



  console.log('Memphis: loading Power BI dashboard...');

  await page.goto(

    'https://app.powerbigov.us/view?r=eyJrIjoiZTYyYmQ0Y2QtZTM0Ni00ZTFiLThkMjMtOTYxYWZiOWUyZDU4IiwidCI6IjQxNjQ3NTYxLTY1MzctNDQyMy05NmE5LTg1OWU4OWY4OTE5ZiJ9',

    { waitUntil: 'domcontentloaded', timeout: 60000 }

  );



  // Wait for Power BI content to render

  try {

    await page.waitForFunction(

      () => document.body.innerText.includes('Crime Overview'),

      { timeout: 30000 }

    );

  } catch(e) {

    console.log('Memphis: Crime Overview not found after 30s, proceeding anyway...');

  }

  await page.waitForTimeout(5000);



  // Grab as-of date from page 1: "Data through 2/21/2026" bottom right

  const page1Text = await page.evaluate(() => document.body.innerText);

  console.log('Memphis page1 sample:', page1Text.substring(0, 400));

  let asof = null;

  const dateMatch = page1Text.match(/Data through\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  if (dateMatch) {

    asof = dateMatch[3] + '-' + dateMatch[1].padStart(2,'0') + '-' + dateMatch[2].padStart(2,'0');

    console.log('Memphis as-of:', asof);

  }



  // Click Crime Summary tab

  console.log('Memphis: clicking Crime Summary tab...');

  try {

    await page.getByText('Crime Summary').first().click();

  } catch(e) {

    await page.locator('text=Crime Summary').first().click();

  }

  await page.waitForTimeout(4000);



  // Click Non-Fatal Shooting button

  console.log('Memphis: clicking Non-Fatal Shooting...');

  await page.locator('text=Non-Fatal').first().click();

  await page.waitForTimeout(6000); // extra wait for chart to render



  // Read chart text + take screenshot for vision fallback

  const chartText = await page.evaluate(() => document.body.innerText);

  console.log('Memphis chart sample:', chartText.substring(0, 800));

  const screenshotBuf = await page.screenshot({ fullPage: false });

  console.log('Memphis: screenshot taken, size:', screenshotBuf.length, 'bytes');



  await browser.close();



  const yr = new Date().getFullYear();



  let ytd = null, prior = null;



  var ytdMatch = chartText.match(new RegExp(yr + ':\\s*(\\d+)(?=\\s|\\(|$)'));

  var priorMatch = chartText.match(new RegExp((yr-1) + ':\\s*(\\d+)(?=\\s|\\(|$)'));

  if (ytdMatch) {

    ytd = parseInt(ytdMatch[1]);

    console.log('Memphis: found YTD from title: ' + yr + ': ' + ytd);

  }

  if (priorMatch) {

    prior = parseInt(priorMatch[1]);

    console.log('Memphis: found prior from title: ' + (yr-1) + ': ' + prior);

  }



  if (ytd !== null && ytd > 999) {

    console.log('Memphis: implausible ytd=' + ytd + ', resetting to null for vision fallback');

    ytd = null;

  }



  if (ytd === null) {

    console.log('Memphis: text parsing failed, using vision API...');

    const base64Image = screenshotBuf.toString('base64');

    const visionText = await callClaudeVision(base64Image, 'image/png',
      'This is a Memphis Non-Fatal Shooting Incidents bar chart. The chart title area shows "YEAR: COUNT" for the current and prior year. What are the two values? Reply ONLY in this format: YTD=N PRIOR=N (where YTD is the current/latest year count and PRIOR is the previous year count)',
      { maxTokens: 64 });

    console.log('Memphis vision response:', visionText);



    var vYtd = visionText.match(/YTD=(\d+)/);

    var vPrior = visionText.match(/PRIOR=(\d+)/);

    if (vYtd) ytd = parseInt(vYtd[1]);

    if (vPrior) prior = parseInt(vPrior[1]);

  }



  console.log('Memphis parsed: ytd=' + ytd + ' prior=' + prior);



  if (ytd === null) throw new Error('Could not find ' + yr + ' Non-Fatal Shooting value. Chart text sample: ' + chartText.substring(0, 800));



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

async function _fetchGIVEAllImpl() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const results = {};

  // Process each city in a separate page using URL parameter filtering
  const cityKeys = Object.keys(GIVE_CITIES);
  for (var ci = 0; ci < cityKeys.length; ci++) {
    var key = cityKeys[ci];
    var city = GIVE_CITIES[key];
    console.log('\nGIVE: === Processing ' + city.label + ' (' + city.jurisdiction + ') ===');

    var page = await browser.newPage();
    await page.setViewportSize({ width: 1536, height: 1024 });
    page.setDefaultTimeout(30000);

    try {
      // Load Shooting Activity with jurisdiction pre-filtered via URL parameter
      var url = 'https://mypublicdashboard.ny.gov/t/OJRP_PUBLIC/views/GIVEInitiative/ShootingActivity?Jurisdiction=' + encodeURIComponent(city.jurisdiction);
      console.log(city.label + ': loading ' + url);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(15000);

      // Switch to Monthly Data
      console.log(city.label + ': clicking Monthly Data...');
      await page.locator('text=Monthly Data').first().click({ force: true });
      await page.waitForTimeout(8000);

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

      // Parse CSV
      results[key] = parseGIVECsv(csvText, city.label);
      console.log(city.label + ': OK — ytd=' + results[key].ytd + ' prior=' + results[key].prior);

    } catch(e) {
      console.log(city.label + ': FAILED — ' + e.message);
      results[key] = null;
    }

    await page.close();
  }

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

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  await page.setViewportSize({ width: 1536, height: 768 });

  page.setDefaultTimeout(30000);



  const url = 'https://www.miamidade.gov/global/police/crime-stats.page';

  console.log('MiamiDade: loading wrapper page...');

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // Wait for Power BI iframe to appear instead of fixed 10s
  try {
    await page.waitForSelector('iframe[src*="powerbi"]', { timeout: 15000 });
    await page.waitForTimeout(2000);
  } catch {
    await page.waitForTimeout(8000);
  }

  const iframeSrc = await page.evaluate(() => {

    const frames = Array.from(document.querySelectorAll('iframe'));

    const pbi = frames.find(f => f.src && f.src.includes('powerbi'));

    return pbi ? pbi.src : null;

  });

  console.log('MiamiDade iframe src:', iframeSrc);



  if (!iframeSrc) {

    const src = await page.content();

    console.log('MiamiDade page source snippet:', src.substring(0, 2000));

    await browser.close();

    throw new Error('Could not find Power BI iframe on Miami-Dade page');

  }



  console.log('MiamiDade: loading Power BI embed directly...');

  await page.goto(iframeSrc, { waitUntil: 'domcontentloaded', timeout: 60000 });

  await waitForPowerBI(page, 15000);



  const page1Text = await page.evaluate(() => document.body.innerText);

  console.log('MiamiDade PBI page1 sample:', page1Text.substring(0, 600));



  const dateMatch = page1Text.match(/Last update dat[ae][:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  let asof = null;

  if (dateMatch) {

    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;

  }

  console.log('MiamiDade asof:', asof);



  console.log('MiamiDade: navigating to page 3...');

  for (let i = 0; i < 2; i++) {

    try {

      await page.locator('.pbi-glyph-chevronrightmedium').first().click({ force: true, timeout: 5000 });

      await page.waitForTimeout(5000);

      console.log(`MiamiDade: clicked next (${i+1}/2)`);

    } catch(e) {

      try {

        await page.locator('[aria-label="Next page"]').first().click({ force: true, timeout: 3000 });

        await page.waitForTimeout(5000);

        console.log(`MiamiDade: clicked Next page button (${i+1}/2)`);

      } catch(e2) {

        console.log(`MiamiDade: nav click ${i+1} failed:`, e.message);

      }

    }

  }



  const page3Text = await page.evaluate(() => document.body.innerText);

  console.log('MiamiDade page3 sample:', page3Text.substring(0, 1000));

  await browser.close();



  if (!asof) {

    const dateMatch3 = page3Text.match(/Last update dat[ae][:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

    if (dateMatch3) {

      asof = `${dateMatch3[3]}-${dateMatch3[1].padStart(2,'0')}-${dateMatch3[2].padStart(2,'0')}`;

      console.log('MiamiDade asof from page3:', asof);

    }

  }



  const yr = new Date().getFullYear();

  let ytd = null, prior = null;



  const shootMatch = page3Text.match(/SHOOTINGS[\s\S]{0,300}/i);

  if (shootMatch) {

    const nums = [...shootMatch[0].matchAll(/(\d+)\s+[-\d.]+%/g)];

    console.log('MiamiDade shootings nums:', nums.map(m => m[1]).join(', '));

    if (nums.length >= 1) ytd   = parseInt(nums[0][1]);

    if (nums.length >= 2) prior = parseInt(nums[1][1]);

  }



  if (ytd === null) {

    const lines = page3Text.split('\n').map(l => l.trim()).filter(Boolean);

    for (let i = 0; i < lines.length; i++) {

      if (/^SHOOTINGS$/i.test(lines[i])) {

        const vals = [];

        for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {

          if (/^\d+$/.test(lines[j])) vals.push(parseInt(lines[j]));

          if (vals.length === 2) break;

        }

        console.log('MiamiDade line-scan shootings:', vals);

        if (vals.length >= 1) ytd   = vals[0];

        if (vals.length >= 2) prior = vals[1];

        break;

      }

    }

  }



  console.log(`MiamiDade parsed: ytd=${ytd} prior=${prior}`);

  if (ytd === null) throw new Error('Could not parse MiamiDade shootings from page text');



  return { ytd, prior, asof };

}





// ─── Main ─────────────────────────────────────────────────────────────────────



// ─── Omaha (REMOVED - Akamai CDN blocks all automated access) ────────────────






// ─── New Haven (CivicPlus CompStat PDF) ──────────────────────────────────────
// ─── Minneapolis (ArcGIS FeatureServer) ──────────────────────────────────────



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

    // Omaha removed — Akamai blocks all automated access
  ]);



  for (const { key, value } of fetches) {

    if (value.ok) {

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

}



main().catch(e => { console.error(e); process.exit(1); });





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





// ─── Denver (Power BI - Firearm Homicides + Non-Fatal Shootings) ─────────────



async function fetchDenver() {

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  await page.setViewportSize({ width: 1536, height: 900 });

  page.setDefaultTimeout(30000);



  const url = 'https://app.powerbigov.us/view?r=eyJrIjoiOWMwZjg0MGYtODI0ZC00ZGVjLThmNjEtMzExZDI3OGUzYzQyIiwidCI6IjM5Yzg3YWIzLTY2MTItNDJjMC05NjIwLWE2OTZkMTJkZjgwMyJ9';

  console.log('Denver: loading Power BI embed directly...');

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

  await waitForPowerBI(page, 10000);

  const page1Text = await page.evaluate(() => document.body.innerText);

  console.log('Denver page1 sample:', page1Text.substring(0, 600));



  console.log('Denver: navigating to page 3...');

  for (let i = 0; i < 2; i++) {

    try {

      await page.locator('.pbi-glyph-chevronrightmedium').first().click({ force: true, timeout: 5000 });

      await page.waitForTimeout(5000);

      console.log(`Denver: clicked next (${i+1}/2)`);

    } catch(e) {

      try {

        await page.locator('[aria-label="Next page"]').first().click({ force: true, timeout: 3000 });

        await page.waitForTimeout(5000);

        console.log(`Denver: clicked Next page button (${i+1}/2)`);

      } catch(e2) {

        console.log(`Denver: nav click ${i+1} failed:`, e.message);

      }

    }

  }



  await page.waitForTimeout(5000);

  const page3Text = await page.evaluate(() => document.body.innerText);

  console.log('Denver page3 sample:', page3Text.substring(0, 1000));



  let asof = null;

  const dateMatch = page3Text.match(/Last Updated[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  if (dateMatch) {

    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;

  }

  console.log('Denver asof:', asof);



  const yr = new Date().getFullYear();

  let ytd = null, prior = null;



  const ytdMatch = page3Text.match(new RegExp('Firearm Homicides \\+ Non-Fatal Shooting Victims ' + yr + ' YTD[\\s\\n]+(\\d+)', 'i'));

  const priorMatch = page3Text.match(new RegExp('Firearm Homicides \\+ Non-Fatal Shooting Victims ' + (yr-1) + ' YTD[\\s\\n]+(\\d+)', 'i'));



  if (ytdMatch) ytd = parseInt(ytdMatch[1]);

  if (priorMatch) prior = parseInt(priorMatch[1]);

  console.log('Denver text parse: ytd=' + ytd + ' prior=' + prior);



  if (ytd === null) {

    const lines = page3Text.split('\n').map(l => l.trim()).filter(Boolean);

    for (let i = 0; i < lines.length; i++) {

      if (/Firearm Homicides.*Non-Fatal.*\d{4}\s*YTD/i.test(lines[i])) {

        for (let j = Math.max(0, i-2); j < Math.min(lines.length, i+3); j++) {

          if (/^\d+$/.test(lines[j])) {

            if (lines[i].includes(String(yr)) && ytd === null) ytd = parseInt(lines[j]);

            else if (lines[i].includes(String(yr-1)) && prior === null) prior = parseInt(lines[j]);

          }

        }

      }

    }

    console.log('Denver line-scan: ytd=' + ytd + ' prior=' + prior);

  }



  if (ytd === null || prior === null) {

    console.log('Denver: falling back to vision API...');

    const screenshotBuf = await page.screenshot({ fullPage: false });

    await browser.close();

    const base64Image = screenshotBuf.toString('base64');

    const promptText = [
      'This is a Denver Police Department Power BI dashboard showing "Reported Firearm Homicides and Non-Fatal Shootings in Denver".',
      'It shows two main numbers: the current year YTD count and the previous year YTD count for "Firearm Homicides + Non-Fatal Shooting Victims".',
      'It also shows "Last Updated" date in the top right.',
      'Extract: YTD (current year number), PRIOR (previous year number), and ASOF (Last Updated date in YYYY-MM-DD format).',
      'Reply ONLY in this exact format: YTD=N PRIOR=N ASOF=YYYY-MM-DD'
    ].join(' ');

    const visionText = await callClaudeVision(base64Image, 'image/png', promptText, { maxTokens: 128 });

    console.log('Denver vision response:', visionText);



    const ytdV = visionText.match(/YTD=(\d+)/);

    const priorV = visionText.match(/PRIOR=(\d+)/);

    const asofV = visionText.match(/ASOF=(\d{4}-\d{2}-\d{2})/);

    if (ytdV) ytd = parseInt(ytdV[1]);

    if (priorV) prior = parseInt(priorV[1]);

    if (asofV && !asof) asof = asofV[1];

  } else {

    await browser.close();

  }



  console.log('Denver final: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);

  if (ytd === null || prior === null) throw new Error('Denver: could not extract data');

  return { ytd, prior, asof };

}





// ─── Portsmouth (Power BI - GSW Victims) ─────────────────────────────────────



async function fetchPortsmouth() {

  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });

  const page    = await browser.newPage();

  await page.setViewportSize({ width: 1536, height: 900 });

  page.setDefaultTimeout(30000);



  const url = 'https://app.powerbigov.us/view?r=eyJrIjoiZDc3ZmQyYzMtOTgyYi00ODQzLTk4ZWUtZWQyY2ZkODM5ZWNkIiwidCI6ImM3N2RiNGQ4LWEwZjUtNDU0YS05MmMxLWI3ZDg0YzY0ZmQ0NCJ9';

  console.log('Portsmouth: loading Power BI dashboard...');

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

  await waitForPowerBI(page, 15000);



  const bodyText = await page.evaluate(() => document.body.innerText);

  console.log('Portsmouth page sample:', bodyText.substring(0, 800));



  let asof = null;

  const dateMatch = bodyText.match(/(?:Last\s+(?:Database\s+)?Update[d]?|Updated)[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{4})/i);

  if (dateMatch) {

    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;

  }



  const screenshotBuf = await page.screenshot({ fullPage: false });

  await browser.close();

  console.log('Portsmouth: screenshot taken, size:', screenshotBuf.length, 'bytes');



  const base64Image = screenshotBuf.toString('base64');

  const yr = new Date().getFullYear();



  const promptText = [

    'This is a bar chart titled "GSW Victims – Injuries/Death & Rate (YTD)" from Portsmouth Police.',

    'Each year has a group of bars. Above each group is a TOTAL number shown with a dashed orange line.',

    'There is also a small purple bar at the bottom of each group labeled "Fatal (Suicide)" with a small number.',

    '',

    'For the two RIGHTMOST year groups (' + yr + ' and ' + (yr-1) + '), read:',

    '1. The TOTAL number shown at the very top (dashed orange line) - this is the largest/highest number for each year',

    '2. The purple Fatal (Suicide) number - this is usually the smallest number, at the bottom',

    '',

    'Also look for any date indicator like "Last Updated" or a date range on the page.',

    '',

    'Reply ONLY in this exact format:',

    yr + '_TOTAL=N ' + yr + '_SUICIDE=N ' + (yr-1) + '_TOTAL=N ' + (yr-1) + '_SUICIDE=N ASOF=YYYY-MM-DD',

    'If you cannot find a date, omit ASOF.'

  ].join('\n');



  let visionText = await callClaudeVision(base64Image, 'image/png', promptText,
    { model: 'claude-sonnet-4-5-20250929', maxTokens: 128 });

  console.log('Portsmouth vision response:', visionText);

  for (let retry = 1; retry <= 2 && !visionText.includes('TOTAL='); retry++) {

    console.log('Portsmouth: retrying vision call attempt ' + retry + '...');

    await new Promise(r => setTimeout(r, 3000));

    visionText = await callClaudeVision(base64Image, 'image/png', promptText,
      { model: 'claude-sonnet-4-5-20250929', maxTokens: 128 });

    console.log('Portsmouth vision retry ' + retry + ' response:', visionText);

  }



  const ytdTotal = visionText.match(new RegExp(yr + '_TOTAL=(\\d+)'));

  const ytdSui   = visionText.match(new RegExp(yr + '_SUICIDE=(\\d+)'));

  const prTotal  = visionText.match(new RegExp((yr-1) + '_TOTAL=(\\d+)'));

  const prSui    = visionText.match(new RegExp((yr-1) + '_SUICIDE=(\\d+)'));



  console.log('Portsmouth parsed: ' + yr + ' T=' + (ytdTotal?.[1]||'?') + ' S=' + (ytdSui?.[1]||'?') +

    ' | ' + (yr-1) + ' T=' + (prTotal?.[1]||'?') + ' S=' + (prSui?.[1]||'?'));



  const ytd = (ytdTotal && ytdSui) ? parseInt(ytdTotal[1]) - parseInt(ytdSui[1]) : null;

  const prior = (prTotal && prSui) ? parseInt(prTotal[1]) - parseInt(prSui[1]) : null;



  if (!asof) {

    const asofV = visionText.match(/ASOF=(\d{4}-\d{2}-\d{2})/);

    if (asofV) asof = asofV[1];

  }



  console.log('Portsmouth final: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);

  if (ytd === null || prior === null) throw new Error('Portsmouth: vision parse failed: ' + visionText);

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

