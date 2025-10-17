/**
 * Jupiter proxy (Cloudflare Worker)
 * - Back-compat: /v6/* -> /swap/v1/*
 * - Auth: prefer header `x-jup-key: <key>`; also accept `x-jup-auth: Bearer <key>`, or query ?key=/ ?auth=
 * - Routing: Pro first, then Lite fallback (Lite only used if Pro returns 4xx JSON or a non-JSON failure)
 * - Guardrail: if a request tries to use classic USDC (EPjF…), rewrite to CCTP USDC (DezX…) and mark it in headers
 */

const LITE_BASE = "https://lite-api.jup.ag";
const PRO_BASE  = "https://api.jup.ag";

// Classic vs CCTP USDC
const USDC_CLASSIC = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v";
const USDC_CCTP    = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";

/* ───────────────────────── helpers ───────────────────────── */

const asText = (s, t, extra = {}) =>
  new Response(t, { status: s, headers: { "content-type": "text/plain", ...extra } });

const asJSON = (s, o, extra = {}) =>
  new Response(JSON.stringify(o), { status: s, headers: { "content-type": "application/json", ...extra } });

function corsify(resp) {
  const h = new Headers(resp.headers);
  h.set("access-control-allow-origin", "*");
  h.set("access-control-expose-headers", "*");
  return new Response(resp.body, { status: resp.status, headers: h });
}

// /v6/* -> /swap/v1/* (also accept direct /swap/v1/*)
function normalizePath(inPathname, queryPath) {
  let p = queryPath || inPathname || "";
  if (p.startsWith("/v6/")) {
    if (p === "/v6/quote") p = "/swap/v1/quote";
    else if (p === "/v6/swap") p = "/swap/v1/swap";
    else p = p.replace(/^\/v6\//, "/swap/v1/");
  }
  if (!p.startsWith("/swap/v1/")) return null;
  return p;
}

/** Extract a Pro token (raw key; no "Bearer ") from header or query */
function pickProToken(requestURL, requestHeaders) {
  // Preferred: x-jup-key: <key>
  let raw = requestHeaders.get("x-jup-key");
  if (raw) return raw.trim();

  // Compat: x-jup-auth: Bearer <key>
  raw = requestHeaders.get("x-jup-auth");
  if (raw) return raw.replace(/^Bearer\s+/i, "").trim();

  // Query: ?key=<key>  or  ?auth=Bearer%20<key>
  raw = requestURL.searchParams.get("key");
  if (raw) return raw.trim();

  raw = requestURL.searchParams.get("auth");
  if (raw) return raw.replace(/^Bearer\s+/i, "").trim();

  return null;
}

// Build headers to forward upstream (strip hop-by-hop; map our auth to x-api-key for Pro)
function buildForwardHeaders(req, proKeyOrNull) {
  const h = new Headers(req.headers);

  // strip hop-by-hop / CF headers
  ["host","cf-connecting-ip","cf-ray","cf-visitor","cf-ew-via","cf-ipcountry","content-length"].forEach(k => h.delete(k));

  // be explicit about JSON & browser-ish UA
  h.set("accept", "application/json");
  if (!h.has("user-agent")) h.set("user-agent", "Mozilla/5.0 PNLSniper-Worker/1.0");
  if (!h.has("origin"))  h.set("origin", "https://jup.ag");
  if (!h.has("referer")) h.set("referer", "https://jup.ag");

  // convenience headers should never leak upstream
  h.delete("x-jup-auth");
  h.delete("x-jup-key");

  // only add x-api-key when calling Pro
  if (proKeyOrNull) h.set("x-api-key", proKeyOrNull);

  return h;
}

// Robust “JSON sniff” (works even when upstream lies about content-type)
async function sniffJson(resp) {
  const ct = (resp.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("application/json")) return { isJson: true, rebuilt: resp };

  const r = resp.body?.getReader?.();
  if (!r) return { isJson: false, rebuilt: resp };

  const { value } = await r.read();           // peek first chunk
  const first = value || new Uint8Array();

  // Rebuild the stream so caller can still read it
  const rebuilt = new Response(
    new ReadableStream({
      start(controller) {
        if (first.length) controller.enqueue(first);
        resp.body?.pipeTo(new WritableStream({
          write(c) { controller.enqueue(c); },
          close()  { controller.close(); },
          abort()  { controller.close(); },
        })).catch(() => controller.close());
      }
    }),
    { status: resp.status, headers: resp.headers }
  );

  const txt = new TextDecoder().decode(first);
  const isJson = /^\s*[{[]/.test(txt);
  return { isJson, rebuilt };
}

/* ─────────── Guardrail: map classic USDC → CCTP (GET + POST/quote JSON) ─────────── */

function guardrailQueryParams(qp) {
  let changed = false;
  const inp = qp.get("inputMint");
  const out = qp.get("outputMint");
  if (inp === USDC_CLASSIC) { qp.set("inputMint", USDC_CCTP); changed = true; }
  if (out === USDC_CLASSIC) { qp.set("outputMint", USDC_CCTP); changed = true; }
  return changed;
}

function guardrailBodyMaybeRewriteJSON(jsonBody) {
  // Only rewrite simple quote requests: { inputMint, outputMint, ... }
  // (We do NOT attempt to rewrite deep quoteResponse objects for /swap to avoid mismatch.)
  if (jsonBody && typeof jsonBody === "object" && !Array.isArray(jsonBody)) {
    let changed = false;
    if (jsonBody.inputMint === USDC_CLASSIC) { jsonBody.inputMint = USDC_CCTP; changed = true; }
    if (jsonBody.outputMint === USDC_CLASSIC) { jsonBody.outputMint = USDC_CCTP; changed = true; }
    return changed;
  }
  return false;
}

/* ───────────────────────── worker ───────────────────────── */

export default {
  async fetch(request) {
    // CORS preflight (handy for local testing / Postman / browser)
    if (request.method === "OPTIONS") {
      return corsify(new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-allow-headers": "*",
          "access-control-max-age": "600"
        }
      }));
    }

    try {
      const url = new URL(request.url);

      /* ── health & debug ── */
      if (url.pathname === "/healthz")
        return corsify(asText(200, "ok"));

      if (url.pathname === "/debug/edge") {
        const t = await fetch("https://cloudflare.com/cdn-cgi/trace").then(r => r.text()).catch(() => "(no trace)");
        return corsify(asText(200, `OK\nworker_url=${request.url}\n\n${t}`));
      }

      if (url.pathname === "/debug/echo") {
        const k = pickProToken(url, request.headers);
        return corsify(asJSON(200, {
          saw_pro_header: !!request.headers.get("x-jup-auth") || !!request.headers.get("x-jup-key"),
          saw_pro_query : !!(url.searchParams.get("auth") || url.searchParams.get("key")),
          token_preview : k ? (k.slice(0, 6) + "…(len " + k.length + ")") : null
        }));
      }

      if (url.pathname === "/debug/try") {
        const p  = normalizePath("/swap/v1/quote", url.searchParams.get("path"));
        if (!p) return corsify(asJSON(400, { error: "missing/invalid path" }));
        const qp = new URLSearchParams(url.searchParams);
        qp.delete("path"); qp.delete("auth"); qp.delete("key");

        // Guardrail (query)
        const gr = guardrailQueryParams(qp);

        const token = pickProToken(url, request.headers);

        const out = [];
        // Pro first (if token), then Lite
        if (token) {
          const h = buildForwardHeaders(request, token);
          const target = `${PRO_BASE}${p}?${qp}`;
          const r = await fetch(target, { method: "GET", headers: h }).catch(e => new Response(String(e), { status: 599 }));
          const head = await r.clone().text().then(s => s.slice(0, 160)).catch(() => "");
          out.push({ base: PRO_BASE, status: r.status, ct: r.headers.get("content-type") || "", head });
        }
        {
          const h = buildForwardHeaders(request, null);
          const target = `${LITE_BASE}${p}?${qp}`;
          const r = await fetch(target, { method: "GET", headers: h }).catch(e => new Response(String(e), { status: 599 }));
          const head = await r.clone().text().then(s => s.slice(0, 160)).catch(() => "");
          out.push({ base: LITE_BASE, status: r.status, ct: r.headers.get("content-type") || "", head });
        }
        return corsify(asJSON(200, { tried: out, guardrail_query_rewrite: gr }));
      }

      /* ── path rewrite ── */
      const jupPath = normalizePath(url.pathname, url.searchParams.get("path"));
      if (!jupPath)
        return corsify(asJSON(400, { error: "missing/invalid path (need /v6/... or /swap/v1/...)" }));

      // strip internal keys from query
      const qp = new URLSearchParams(url.searchParams);
      qp.delete("path"); qp.delete("auth"); qp.delete("key");

      // Guardrail (query): rewrite classic USDC → CCTP
      const guardQuery = guardrailQueryParams(qp);

      // body passthrough (+ optional guardrail on simple JSON)
      const method = request.method.toUpperCase();
      let body;
      let guardBody = false;
      if (method === "POST") {
        const ct = (request.headers.get("content-type") || "").toLowerCase();
        if (ct.includes("application/json")) {
          const js = await request.json().catch(() => ({}));
          guardBody = guardrailBodyMaybeRewriteJSON(js);
          body = JSON.stringify(js);
        } else {
          body = await request.arrayBuffer();
        }
      }

      // Pro first when a key is present; Lite fallback
      const proToken = pickProToken(url, request.headers);
      const bases = proToken ? [PRO_BASE, LITE_BASE] : [LITE_BASE];

      let lastNonJson = null;
      const tried = [];

      for (const base of bases) {
        const target = `${base}${jupPath}${qp.toString() ? `?${qp}` : ""}`;
        tried.push(target);

        const headers = buildForwardHeaders(request, base === PRO_BASE ? proToken : null);
        // Surface guardrail info to callers
        if (guardQuery || guardBody) {
          headers.set("x-jup-guard", "usdc-classic-remapped");
        }

        let upstream = await fetch(target, { method, headers, body })
          .catch(err => new Response(String(err), { status: 599, headers: { "content-type": "text/plain" } }));

        // Be generous with JSON
        const probe = await sniffJson(upstream);
        upstream = probe.rebuilt;

        if (probe.isJson) {
          // If Pro returns a 4xx JSON, try Lite before returning the error
          if (base === PRO_BASE && proToken && upstream.status >= 400 && upstream.status < 500) {
            lastNonJson = upstream;
            continue;
          }
          const outHeaders = new Headers(upstream.headers);
          if (!outHeaders.get("content-type")) outHeaders.set("content-type", "application/json");
          outHeaders.set("x-jup-upstream", base);
          outHeaders.set("x-jup-auth-mode", proToken ? (base === PRO_BASE ? "pro" : "lite-fallback") : "lite");
          if (guardQuery || guardBody) outHeaders.set("x-jup-guard", "usdc-classic-remapped");
          return corsify(new Response(upstream.body, { status: upstream.status, headers: outHeaders }));
        }

        // Non-JSON (HTML errors, 1016, etc.): remember & try next base
        lastNonJson = upstream;
      }

      // If we get here, neither base yielded JSON; return the last thing we saw
      if (lastNonJson) {
        const text = await lastNonJson.text().catch(() => "");
        return corsify(asText(lastNonJson.status || 502, text, {
          "x-error": "upstreams-failed-or-nonjson",
          "x-jup-tried": tried.join(" | ")
        }));
      }

      return corsify(asText(502, "no upstreams responded", { "x-error": "no-response" }));
    } catch (e) {
      return corsify(asJSON(500, { error: String(e?.message || e) }));
    }
  }
};
