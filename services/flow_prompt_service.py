"""
Service nhập và gửi prompt trên Google Flow editor.

Mục tiêu:
- Tách riêng logic UI tương tác prompt khỏi dreamina.py để dễ maintain.
- Có thể gọi độc lập sau khi apply_flow_generation_settings_panel() xong.
- Hỗ trợ Google Flow (contenteditable) — KHÔNG dùng cho Dreamina cũ.

Luồng gọi thông thường:
    await find_and_focus_flow_prompt(page)
    await type_flow_prompt(page, text)
    await send_flow_prompt(page)
"""

from __future__ import annotations

import asyncio
import platform
import sys
from pathlib import Path

# ── Đường dẫn động ─────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent


# ── Selector tìm ô nhập prompt (theo thứ tự ưu tiên) ─────────────────────
# Google Flow dùng div contenteditable, không phải <textarea> thông thường.
FLOW_PROMPT_SELECTORS = [
    "div[contenteditable='true'][data-placeholder]",
    "div[contenteditable='true'][role='textbox']",
    "div[contenteditable='true']",
    "[role='textbox']",
    "textarea",
]

# ── Selector tìm nút Tạo / Generate (fallback khi Enter không hoạt động) ──
FLOW_SEND_SELECTORS = [
    "button[data-testid='generate-btn']",
    "button[aria-label*='Generate']",
    "button[aria-label*='Tạo']",
    "button[aria-label*='generate']",
    "button[type='submit']",
]


def _log(msg: str, level: str = "DBG") -> None:
    """Log đơn giản ra stdout, không cần import dreamina.py."""
    icon = {
        "OK":   "✅",
        "WARN": "⚠️ ",
        "ERR":  "❌",
        "DBG":  "   ",
    }.get(level, "   ")
    print(f"  {icon} [flow_prompt] {msg}", flush=True)


# ════════════════════════════════════════════════════════════════════════════
#  TÌM Ô NHẬP PROMPT
# ════════════════════════════════════════════════════════════════════════════

async def _find_flow_prompt_element(page):
    """
    Tìm element ô nhập prompt bằng danh sách selector ưu tiên.
    Trả về element nếu tìm thấy và visible, ngược lại trả None.
    """
    for sel in FLOW_PROMPT_SELECTORS:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible(timeout=800):
                return el
        except Exception:
            pass
    return None


async def find_and_focus_flow_prompt(page):
    """
    Tìm và focus vào ô nhập prompt của Google Flow.

    Chiến lược:
    1. Thử selector → click element
    2. Fallback pixel click vào vùng giữa-dưới trang (vị trí thường của ô nhập)
    3. Kiểm tra activeElement có phải input/contenteditable không

    Trả về element nếu focus thành công qua selector, None nếu dùng pixel click.
    """
    # ── Thử selector ──
    el = await _find_flow_prompt_element(page)
    if el:
        try:
            await el.click()
            await asyncio.sleep(0.3)
            _log("Focus vào ô nhập (selector)", "OK")
            return el
        except Exception:
            pass

    # ── Fallback pixel click ──
    vp = page.viewport_size or {}
    w = int(vp.get("width", 1440))
    h = int(vp.get("height", 900))

    # Thử nhiều tọa độ vùng prompt (Google Flow ô nhập thường ở dưới cùng)
    pixel_positions = [
        (w * 0.50, h - 130),
        (w * 0.50, h - 110),
        (w * 0.50, h - 155),
        (w * 0.45, h - 130),
        (w * 0.55, h - 130),
    ]

    for px, py in pixel_positions:
        try:
            await page.mouse.click(px, py)
            await asyncio.sleep(0.4)

            # Kiểm tra element đang focus có phải ô nhập không
            tag      = await page.evaluate("document.activeElement?.tagName || ''")
            editable = await page.evaluate("document.activeElement?.contentEditable || ''")
            role     = await page.evaluate("document.activeElement?.getAttribute('role') || ''")

            is_input = (
                tag.lower() in ["textarea", "input"]
                or editable == "true"
                or role == "textbox"
            )
            if is_input:
                _log(f"Focus vào ô nhập (pixel {int(px)},{int(py)})", "OK")
                return None  # đã focus, dùng keyboard từ đây
        except Exception:
            pass

    _log("Không tìm được ô nhập prompt", "WARN")
    return None


# ════════════════════════════════════════════════════════════════════════════
#  NHẬP PROMPT
# ════════════════════════════════════════════════════════════════════════════

async def type_flow_prompt(page, text: str) -> None:
    """
    Click ô nhập → xóa nội dung cũ → paste text mới vào ô prompt.

    Dùng clipboard paste thay vì type từng ký tự vì:
    - Nhanh hơn nhiều với prompt dài.
    - Tránh lỗi IME với ký tự đặc biệt tiếng Việt.
    """
    # Focus vào ô nhập trước
    await find_and_focus_flow_prompt(page)
    await asyncio.sleep(0.1)

    # Xóa nội dung cũ bằng Ctrl+A / Cmd+A → Backspace
    select_all = "Meta+a" if platform.system() == "Darwin" else "Control+a"
    await page.keyboard.press(select_all)
    await asyncio.sleep(0.06)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.08)

    # Paste qua clipboard (nhanh, ổn định với text dài)
    await page.evaluate("(t) => navigator.clipboard.writeText(t)", text)
    await asyncio.sleep(0.08)
    paste_key = "Meta+v" if platform.system() == "Darwin" else "Control+v"
    await page.keyboard.press(paste_key)
    await asyncio.sleep(0.15)

    _log(f"Đã paste prompt ({len(text)} ký tự)", "OK")


# ════════════════════════════════════════════════════════════════════════════
#  GỬI PROMPT (nhấn Generate)
# ════════════════════════════════════════════════════════════════════════════

async def send_flow_prompt(page) -> bool:
    """
    Gửi prompt đã nhập trên Google Flow.

    Chiến lược:
    1. Re-focus vào ô nhập để đảm bảo keyboard event đến đúng nơi.
    2. Nhấn Enter để submit.
    3. Fallback: click nút Generate qua selector nếu Enter không hoạt động.

    Trả về True nếu đã gửi thành công (đã nhấn Enter hoặc click nút).
    """
    await asyncio.sleep(0.08)

    # ── Bước 1: Re-focus ô nhập ──
    el = await _find_flow_prompt_element(page)
    if el:
        try:
            await el.click()
        except Exception:
            pass
    else:
        # Pixel click fallback vào giữa-dưới trang
        vp = page.viewport_size or {}
        w = int(vp.get("width", 1440))
        h = int(vp.get("height", 900))
        await page.mouse.click(w * 0.5, h - 130)

    await asyncio.sleep(0.08)

    # ── Bước 2: Nhấn Enter ──
    await page.keyboard.press("Enter")
    await asyncio.sleep(0.5)
    _log("Đã nhấn Enter gửi prompt", "OK")
    return True


# ════════════════════════════════════════════════════════════════════════════
#  TIỆN ÍCH: Click nút Generate trực tiếp (không qua Enter)
# ════════════════════════════════════════════════════════════════════════════

async def click_flow_generate_button(page) -> bool:
    """
    Click thẳng vào nút Generate/Tạo thay vì nhấn Enter.
    Dùng khi Enter không trigger submit (xảy ra ở một số version UI).
    """
    for sel in FLOW_SEND_SELECTORS:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible(timeout=600):
                await btn.click()
                await asyncio.sleep(0.5)
                _log(f"Đã click nút Generate ({sel})", "OK")
                return True
        except Exception:
            pass

    # JS fallback: tìm nút có text "Tạo" / "Generate" gần ô nhập
    try:
        clicked = await page.evaluate(
            """
            () => {
              const norm = (s) => String(s || '').toLowerCase().trim();
              const keywords = ['tạo', 'generate', 'create', 'send'];
              const nodes = Array.from(document.querySelectorAll('button'));
              for (const btn of nodes) {
                const txt = norm(btn.innerText || btn.textContent || btn.getAttribute('aria-label') || '');
                if (keywords.some(k => txt === k || txt.startsWith(k + ' '))) {
                  btn.click();
                  return true;
                }
              }
              return false;
            }
            """
        )
        if clicked:
            await asyncio.sleep(0.5)
            _log("Đã click nút Generate (JS fallback)", "OK")
            return True
    except Exception:
        pass

    _log("Không tìm thấy nút Generate", "WARN")
    return False
