import asyncio
import html
import os
from typing import List, Dict, Any, Optional


def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(lambda: asyncio.run(coro))
            return fut.result()
    return asyncio.run(coro)


async def _html_to_png_bytes_async(html_content: str, width: int = 1080) -> bytes:
    from playwright.async_api import async_playwright

    launch_args = []
    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RENDER") or os.getenv("DYNO"):
        launch_args = ["--no-sandbox"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(args=launch_args)
        page = await browser.new_page(viewport={"width": int(width), "height": 600})
        await page.set_content(html_content, wait_until="load")
        height = await page.evaluate("Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)")
        height = int(max(600, min(height, 6000)))
        await page.set_viewport_size({"width": int(width), "height": height})
        png = await page.screenshot(full_page=True, type="png")
        await browser.close()
        return png


def html_to_png_bytes(html_content: str, width: int = 1080) -> bytes:
    return _run(_html_to_png_bytes_async(html_content, width=width))


def build_factor_share_html(
    factor_label: str,
    factor_code: str,
    race_day_iso: str,
    races: List[Dict[str, Any]],
) -> str:
    title = html.escape(str(factor_label or factor_code))
    code = html.escape(str(factor_code or ""))
    day = html.escape(str(race_day_iso or ""))

    items = []
    for r in races:
        rn = html.escape(str(r.get("race_no") or ""))
        top5 = r.get("top5") or []
        pills = []
        for x in top5[:5]:
            pills.append(f"<span class='pill'>{html.escape(str(x))}</span>")
        pills_html = "".join(pills) if pills else "<span class='muted'>無快照</span>"
        items.append(
            f"""
            <div class="row">
              <div class="race">第 {rn} 場</div>
              <div class="top5">{pills_html}</div>
            </div>
            """
        )

    body = "".join(items) if items else "<div class='muted'>無快照</div>"

    return f"""<!doctype html>
<html lang="zh-HK">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 40px;
      background: #f6f7fb;
      font-family: -apple-system, BlinkMacSystemFont, "Noto Sans HK", "Microsoft JhengHei", "PingFang HK", "Segoe UI", Arial, sans-serif;
      color: #111827;
    }}
    .card {{
      width: 100%;
      max-width: 1000px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #e5e7eb;
      border-radius: 18px;
      padding: 28px 28px 18px 28px;
    }}
    .header {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
      padding-bottom: 16px;
      border-bottom: 1px solid #eef2f7;
    }}
    .h1 {{
      font-size: 28px;
      font-weight: 800;
      margin: 0;
      line-height: 1.2;
    }}
    .sub {{
      font-size: 14px;
      color: #6b7280;
      margin-top: 6px;
    }}
    .meta {{
      text-align: right;
      font-size: 14px;
      color: #374151;
      white-space: nowrap;
    }}
    .meta .k {{
      color: #6b7280;
    }}
    .list {{
      padding-top: 16px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    .row {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 12px 14px;
      background: #f9fafb;
      border: 1px solid #eef2f7;
      border-radius: 12px;
    }}
    .race {{
      font-weight: 700;
      min-width: 90px;
    }}
    .top5 {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 44px;
      height: 34px;
      padding: 0 10px;
      border-radius: 999px;
      background: #111827;
      color: #ffffff;
      font-weight: 800;
      font-size: 16px;
      letter-spacing: 0.2px;
    }}
    .muted {{
      color: #9ca3af;
      font-size: 14px;
    }}
    .footer {{
      padding-top: 14px;
      font-size: 12px;
      color: #9ca3af;
      text-align: right;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <div>
        <div class="h1">{title}</div>
        <div class="sub">獨立條件 Top5 快照</div>
      </div>
      <div class="meta">
        <div><span class="k">賽日：</span>{day}</div>
        <div><span class="k">代號：</span>{code}</div>
      </div>
    </div>
    <div class="list">{body}</div>
    <div class="footer">HKJC Analytics</div>
  </div>
</body>
</html>
"""
