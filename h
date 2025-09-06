[1mdiff --git a/scanner/pumpfun_scanner.py b/scanner/pumpfun_scanner.py[m
[1mindex c781aa7..072c6dd 100644[m
[1m--- a/scanner/pumpfun_scanner.py[m
[1m+++ b/scanner/pumpfun_scanner.py[m
[36m@@ -1,4 +1,4 @@[m
[31m-# scanner/pumpfun_scanner.py — v9[m
[32m+[m[32m# scanner/pumpfun_scanner.py — v10[m
 # Apex-first (next-data → DOM → HTML) with API mirror + fallback and proxy/cookie awareness.[m
 from __future__ import annotations[m
 [m
[36m@@ -40,10 +40,10 @@[m [mexcept Exception:  # pragma: no cover[m
     async def browser_fetch_json(_url: str):  # type: ignore[m
         return None[m
 [m
[31m-    async def browser_fetch_trending_via_next_data(_limit: int = 50):  # type: ignore[m
[32m+[m[32m    async def browser_fetch_trending_via_next_data():  # type: ignore[m
         return None[m
 [m
[31m-    async def browser_scrape_trending_dom(_limit: int = 50):  # type: ignore[m
[32m+[m[32m    async def browser_scrape_trending_dom():  # type: ignore[m
         return None[m
 [m
 [m
[36m@@ -64,6 +64,9 @@[m [mSKIP_NEXT = (os.getenv("PUMPFUN_SKIP_NEXT", "").strip().lower() in ("1", "true",[m
 REQ_TIMEOUT_S = int(os.getenv("PUMPFUN_HTTP_TIMEOUT_S", "15"))[m
 SESSION_LIMIT = int(os.getenv("PUMPFUN_HTTP_CONN_LIMIT", "30"))[m
 [m
[32m+[m[32m# **New**: cap API batch size (fallback path only)[m
[32m+[m[32mAPI_LIMIT = int(os.getenv("PUMPFUN_API_LIMIT", "20"))[m
[32m+[m
 # Preferred API bases: env override → mirror → primary[m
 def _api_bases() -> List[str]:[m
     bases: List[str] = [][m
[36m@@ -93,17 +96,13 @@[m [mdef _host_of(url: str) -> str:[m
         return ""[m
 [m
 def _hdrs(url: str) -> Dict[str, str]:[m
[31m-    host = _host_of(url)[m
[31m-    # OLD:[m
[31m-    # origin = "https://frontend-api.pump.fun" if host.endswith("frontend-api.pump.fun") else "https://pump.fun"[m
[31m-    # NEW (always look like requests were initiated from pump.fun):[m
[31m-    origin = "https://pump.fun"[m
[32m+[m[32m    # Always look like requests were initiated from pump.fun (helps with CF)[m
     h = {[m
         "User-Agent": UA,[m
         "Accept": "application/json, text/plain, */*",[m
         "Accept-Language": "en-US,en;q=0.9",[m
         "Referer": "https://pump.fun/trending",[m
[31m-        "Origin": origin,[m
[32m+[m[32m        "Origin": "https://pump.fun",[m
         "Cache-Control": "no-cache",[m
         "Pragma": "no-cache",[m
     }[m
[36m@@ -119,7 +118,6 @@[m [mdef _hdrs(url: str) -> Dict[str, str]:[m
 def _now() -> float:[m
     return time.time()[m
 [m
[31m-[m
 def _age_min(created_ts: Optional[float]) -> Optional[float]:[m
     try:[m
         if created_ts is None:[m
[36m@@ -130,7 +128,6 @@[m [mdef _age_min(created_ts: Optional[float]) -> Optional[float]:[m
     except Exception:[m
         return None[m
 [m
[31m-[m
 def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:[m
     try:[m
         mint = ([m
[36m@@ -184,7 +181,6 @@[m [mdef _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:[m
     except Exception:[m
         return None[m
 [m
[31m-[m
 def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:[m
     items: List[Dict[str, Any]] = [][m
 [m
[36m@@ -229,6 +225,7 @@[m [masync def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optiona[m
             first = text[:120].replace("\n", " ")[m
             ctype = (r.headers.get("content-type") or "").lower()[m
 [m
[32m+[m[32m            # If CF/HTML shows up, try browser path once, else bubble status[m
             if status in (403, 429, 530) or "text/html" in ctype or text.lstrip().startswith("<!DOCTYPE html"):[m
                 if status in (403, 429, 530):[m
                     flag_cookie_bad()[m
[36m@@ -252,18 +249,17 @@[m [masync def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optiona[m
         LOG.warning("Pump.fun request failed: %s %s", url, e)[m
         return (None, 0, "")[m
 [m
[31m-[m
 def _join_url(base: str, path: str) -> str:[m
     base = base.rstrip("/")[m
     path = path.lstrip("/")[m
     return f"{base}/{path}"[m
 [m
[31m-[m
 async def _fetch_api_json_with_fallback([m
     session: aiohttp.ClientSession,[m
     path: str,[m
     tried_out: Optional[List[str]] = None,[m
 ) -> Optional[List[Dict[str, Any]]]:[m
[32m+[m[32m    # Override URL (debug single endpoint)[m
     if OVERRIDE_URL.lower().startswith("http"):[m
         payload, status, first = await _fetch_json(session, OVERRIDE_URL)[m
         if payload:[m
[36m@@ -296,7 +292,6 @@[m [masync def _fetch_api_json_with_fallback([m
 # ---------------------------------------------------------------------[m
 _BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"[m
 [m
[31m-[m
 def _uniq(seq: List[str]) -> List[str]:[m
     seen = set()[m
     out = [][m
[36m@@ -306,7 +301,6 @@[m [mdef _uniq(seq: List[str]) -> List[str]:[m
             out.append(x)[m
     return out[m
 [m
[31m-[m
 async def _html_scrape_links(session: aiohttp.ClientSession, path: str, limit: int = 50) -> List[Dict[str, Any]]:[m
     url = f"https://pump.fun{path}"[m
     try:[m
[36m@@ -380,7 +374,7 @@[m [masync def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:[m
     # 0) Apex Next.js data (/_next/data) via Playwright (cheap; may be absent)[m
     if not SKIP_NEXT:[m
         try:[m
[31m-            raw = await browser_fetch_trending_via_next_data(limit=50)[m
[32m+[m[32m            raw = await browser_fetch_trending_via_next_data()  # no limit arg[m
             if isinstance(raw, list) and raw:[m
                 items: List[Dict[str, Any]] = [][m
                 for it in raw:[m
[36m@@ -399,9 +393,9 @@[m [masync def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:[m
     # 1) Apex DOM scrape via Playwright (no API host)[m
     if not SKIP_NEXT:[m
         try:[m
[31m-            dom = await browser_scrape_trending_dom(limit=50)[m
[32m+[m[32m            dom = await browser_scrape_trending_dom()  # no limit arg[m
             if isinstance(dom, list) and dom:[m
[31m-                items = [][m
[32m+[m[32m                items: List[Dict[str, Any]] = [][m
                 for it in dom:[m
                     if isinstance(it, dict):[m
                         m = _map_coin(it)[m
[36m@@ -433,7 +427,7 @@[m [masync def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:[m
             if isinstance(m, str) and m not in seen:[m
                 seen.add(m)[m
                 uniq.append(d)[m
[31m-        items = [][m
[32m+[m[32m        items: List[Dict[str, Any]] = [][m
         for it in uniq:[m
             m = _map_coin(it)[m
             if m:[m
[36m@@ -442,16 +436,15 @@[m [masync def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:[m
             LOG.info("scanner.pumpfun: HTML apex → %s items", len(items))[m
             return items[m
 [m
[31m-    # 3) API JSON using mirror/primary fallback[m
[31m-    items = await _fetch_api_json_with_fallback([m
[31m-        session, "/coins/trending?offset=0&limit=50", tried_out=tried[m
[31m-    )[m
[32m+[m[32m    # 3) API JSON using mirror/primary fallback (capped by API_LIMIT)[m
[32m+[m[32m    api_path_tr = f"/coins/trending?offset=0&limit={API_LIMIT}"[m
[32m+[m[32m    api_path_new = f"/coins/new?offset=0&limit={API_LIMIT}"[m
[32m+[m
[32m+[m[32m    items = await _fetch_api_json_with_fallback(session, api_path_tr, tried_out=tried)[m
     if items:[m
         return items[m
 [m
[31m-    items = await _fetch_api_json_with_fallback([m
[31m-        session, "/coins/new?offset=0&limit=50", tried_out=tried[m
[31m-    )[m
[32m+[m[32m    items = await _fetch_api_json_with_fallback(session, api_path_new, tried_out=tried)[m
     if items:[m
         return items[m
 [m
[36m@@ -477,7 +470,7 @@[m [masync def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:[m
         first_base = _api_bases()[0][m
     except Exception:[m
         first_base = "https://frontend-api.pump.fun"[m
[31m-    fallback_url = _join_url(first_base, "/coins/trending?offset=0&limit=50")[m
[32m+[m[32m    fallback_url = _join_url(first_base, f"/coins/trending?offset=0&limit={API_LIMIT}")[m
     payload = await browser_fetch_json(fallback_url)[m
     items = _extract_items_from_payload(payload)[m
     if items:[m
[1mdiff --git a/scripts/smoke_pw.py b/scripts/smoke_pw.py[m
[1mnew file mode 100644[m
[1mindex 0000000..be405dc[m
[1m--- /dev/null[m
[1m+++ b/scripts/smoke_pw.py[m
[36m@@ -0,0 +1,24 @@[m
[32m+[m[32mimport asyncio, os[m
[32m+[m[32mfrom playwright.async_api import async_playwright[m
[32m+[m
[32m+[m[32masync def main():[m
[32m+[m[32m    print("apex via proxy?", os.getenv("PUMPFUN_APEX_VIA_PROXY"))[m
[32m+[m[32m    proxy = os.getenv("PLAYWRIGHT_PROXY")[m
[32m+[m[32m    async with async_playwright() as p:[m
[32m+[m[32m        # Apex: should launch WITHOUT proxy[m
[32m+[m[32m        b1 = await p.chromium.launch(headless=True, proxy=None)[m
[32m+[m[32m        c1 = await b1.new_context()[m
[32m+[m[32m        p1 = await c1.new_page()[m
[32m+[m[32m        await p1.goto("https://pump.fun/board", timeout=120000, wait_until="domcontentloaded")[m
[32m+[m[32m        print("apex title:", await p1.title())[m
[32m+[m[32m        await b1.close()[m
[32m+[m
[32m+[m[32m        # API: should launch WITH proxy[m
[32m+[m[32m        if proxy:[m
[32m+[m[32m            b2 = await p.chromium.launch(headless=True, proxy={"server": proxy})[m
[32m+[m[32m            c2 = await b2.new_context()[m
[32m+[m[32m            r = await c2.request.get("https://frontend-api.pump.fun/health", timeout=30000)[m
[32m+[m[32m            print("api /health:", r.status)[m
[32m+[m[32m            await b2.close()[m
[32m+[m
[32m+[m[32masyncio.run(main())[m
