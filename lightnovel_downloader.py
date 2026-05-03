#!/usr/bin/env python3
"""
轻小说下载器 — LightNovelShelf Downloader
===========================================
基于 SignalR JSON Hub 协议从 lightnovel.life 下载轻小说。支持搜索、
查看信息、下载全书，自动处理网站的自定义字体加密。

依赖
----
    pip install httpx websockets msgpack

用法
----
    # 搜索书籍
    python lightnovel_downloader.py --token TOKEN search "关键词"
    python lightnovel_downloader.py --token TOKEN search "关键词" -p 2    # 翻页

    # 查看书籍信息
    python lightnovel_downloader.py --token TOKEN info 16834

    # 下载全书
    python lightnovel_downloader.py --token TOKEN download 16834           # 默认 HTML
    python lightnovel_downloader.py --token TOKEN download 16834 -f text   # 纯文本
    python lightnovel_downloader.py --token TOKEN download 16834 -o ./out  # 指定输出目录
    python lightnovel_downloader.py --token TOKEN download 16834 -c 10     # 10 并发
    python lightnovel_downloader.py --token TOKEN download 16834 --cover   # 同时下载封面
    python lightnovel_downloader.py --token TOKEN download 16834 -f epub   # EPUB 电子书

    不传 --token 时会交互式提示输入。

输出格式
--------
    html     默认。自动下载加密字体并嵌入 CSS，浏览器打开即可阅读。
    epub     EPUB 3 电子书。自动嵌入字体和封面，可导入阅读器。
    text     纯文本（加密字体无法解码，输出乱码）。
    markdown Markdown 格式（同上限制）。

获取 Token
----------
    登录 https://www.lightnovel.app → F12 控制台 → Application →
    IndexedDB → LightNovelShelf → UserAuthentication → RefreshToken，复制值。

    或在控制台执行:
        const req = indexedDB.open('LightNovelShelf');
        req.onsuccess = () => {
            const tx = req.result.transaction('UserAuthentication');
            tx.objectStore('UserAuthentication').get('RefreshToken')
              .onsuccess = (e) => console.log(e.target.result);
        };

技术说明
--------
    1. 服务端 API 是 ASP.NET Core SignalR Hub（/hub/api），使用 JSON Hub 协议。
    2. 所有请求需要登录认证：RefreshToken 换取 session token → negotiate →
       WebSocket 连接（?access_token=...）。
    3. Hub 方法签名均为 (params, {UseGzip: true})，响应中 Response 字段为
       base64 + gzip 编码的 JSON。
    4. 正文使用自定义 WOFF2 字体加密：将 CJK 字符映射到 Unicode PUA 私用区
       （U+E000~U+F8FF），只有加载对应字体才能正确渲染。本脚本自动下载字体
       并嵌入 HTML，无需额外处理。
"""

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import struct
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
import msgpack
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
log = logging.getLogger("ln_downloader")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = "https://api.lightnovel.life"
CF_API_BASE = "https://cf-api.lightnovel.life"
HUB_PATH = "/hub/api"


# ─── Token 处理 ─────────────────────────────────────────────────────────────


def prompt_token() -> str:
    """交互式提示用户输入 token"""
    print("\n" + "=" * 50)
    print("需要登录认证才能下载/查看小说。")
    print("请登录 https://www.lightnovel.app 后，从浏览器获取 RefreshToken。")
    print("方法: 控制台(F12) → Application → IndexedDB → LightNovelShelf")
    print("    → UserAuthentication → RefreshToken")
    print("=" * 50)
    token = input("\n请输入 RefreshToken: ").strip()
    if not token:
        print("错误: Token 不能为空")
        sys.exit(1)
    return token


async def token_to_session(refresh_token: str) -> str:
    """将 RefreshToken 换取 session token"""
    url = f"{API_BASE}/api/user/refresh_token"
    visitor_id = hashlib.md5(
        f"{random.random()}{time.time()}{random.randint(0, 999999)}".encode()
    ).hexdigest()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-id": visitor_id,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, headers=headers, json={"token": refresh_token})
        if resp.status_code == 401:
            raise PermissionError("token 无效或已过期，请重新登录获取")
        resp.raise_for_status()
        data = resp.json()
        if data.get("Success"):
            session = data.get("Response")
            if session:
                return session
        raise RuntimeError(f"token 无效: {data.get('Msg', '未知错误')}")


# ─── SignalR MessagePack 客户端 ───────────────────────────────────────────


class SignalRClient:
    """轻量级 SignalR MessagePack Hub 协议客户端"""

    MSG_INVOCATION = 1
    MSG_STREAM_ITEM = 2
    MSG_COMPLETION = 3
    MSG_PING = 6
    MSG_CLOSE = 7

    def __init__(self, hub_url: str, token: Optional[str] = None):
        self.hub_url = hub_url.rstrip("/")
        self.token = token
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._invocation_id = 0
        self._pending: dict[str, asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._closed = False
        self._connect_event = asyncio.Event()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def connect(self, max_retries=2):
        for attempt in range(1, max_retries + 1):
            try:
                await self._do_connect()
                return
            except Exception as e:
                if attempt == max_retries:
                    raise
                await asyncio.sleep(min(2 ** attempt, 10))

    async def _do_connect(self):
        connection_token = await self._negotiate()

        ws_scheme = "wss" if self.hub_url.startswith("https") else "ws"
        domain_part = self.hub_url.split("://", 1)[1]
        ws_url = f"{ws_scheme}://{domain_part}?id={quote(connection_token)}"
        if self.token:
            ws_url += f"&access_token={quote(self.token)}"

        self._ws = await websockets.connect(
            ws_url,
            ping_interval=None,
            max_size=10 * 1024 * 1024,
        )

        # SignalR handshake uses JSON with \x1e record separator
        await self._ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
        handshake_raw = await self._ws.recv()
        if isinstance(handshake_raw, bytes):
            handshake_text = handshake_raw.decode("utf-8", errors="replace")
        else:
            handshake_text = handshake_raw
        handshake_text = handshake_text.strip().rstrip("\x1e").strip()
        handshake = json.loads(handshake_text)
        if handshake.get("error"):
            raise RuntimeError(f"握手失败: {handshake['error']}")

        self._closed = False
        self._connect_event.set()
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _negotiate(self) -> str:
        url = f"{self.hub_url}/negotiate?negotiateVersion=1"
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        text = await self._raw_negotiate(url, headers, "POST")
        if text:
            t = self._extract_connection_token(text)
            if t:
                return t

        text = await self._raw_negotiate(f"{self.hub_url}/negotiate", headers, "POST")
        if text:
            t = self._extract_connection_token(text)
            if t:
                return t

        raise RuntimeError(f"协商失败，无法连接服务器: {self.hub_url}")

    async def _raw_negotiate(self, url: str, headers: dict, method: str = "POST") -> Optional[str]:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await (client.post if method == "POST" else client.get)(url, headers=headers)
                if resp.status_code == 401:
                    raise PermissionError("Token 无效或已过期")
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                raw = resp.content
                return raw.decode("utf-8-sig", errors="replace").strip()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            log.warning(f"  negotiate 请求失败: {e}")
            return None

    @staticmethod
    def _extract_connection_token(text: str) -> Optional[str]:
        if not text:
            return None
        try:
            data = json.loads(text)
            token = data.get("connectionToken") or data.get("connectionId")
            if token:
                return token
        except json.JSONDecodeError:
            pass
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(text):
            while idx < len(text) and text[idx] not in "{[":
                idx += 1
            if idx >= len(text):
                break
            try:
                obj, end = decoder.raw_decode(text, idx)
                if isinstance(obj, dict):
                    token = obj.get("connectionToken") or obj.get("connectionId")
                    if token:
                        return token
                idx = end
            except json.JSONDecodeError:
                idx += 1
        return None

    async def invoke(self, target: str, *args) -> Any:
        """使用 JSON 协议发送 SignalR invocation，返回解码后的结果"""
        self._invocation_id += 1
        inv_id = str(self._invocation_id)
        future = asyncio.get_event_loop().create_future()
        self._pending[inv_id] = future

        # JSON 协议格式: {"type":1, "invocationId":"1", "target":"X", "arguments":[...]}
        msg = json.dumps({
            "type": self.MSG_INVOCATION,
            "invocationId": inv_id,
            "target": target,
            "arguments": list(args),
        }) + "\x1e"
        await self._ensure_connected()
        await self._ws.send(msg)
        return await future

    async def _ensure_connected(self):
        if not self._ws or self._closed:
            raise RuntimeError("SignalR 未连接")
        await self._connect_event.wait()

    async def _read_loop(self):
        """JSON 协议读取循环，消息以 \\x1e 分隔"""
        buf = ""
        try:
            async for raw in self._ws:
                if self._closed:
                    break
                if isinstance(raw, bytes):
                    buf += raw.decode("utf-8", errors="replace")
                else:
                    buf += raw
                while "\x1e" in buf:
                    idx = buf.index("\x1e")
                    msg_text = buf[:idx]
                    buf = buf[idx + 1:]
                    if not msg_text.strip():
                        continue
                    try:
                        msg = json.loads(msg_text)
                    except json.JSONDecodeError:
                        continue
                    msg_type = msg.get("type")
                    if msg_type == self.MSG_COMPLETION:
                        self._handle_completion(msg)
                    elif msg_type == self.MSG_PING:
                        await self._ws.send(json.dumps({"type": self.MSG_PING}) + "\x1e")
                    elif msg_type == self.MSG_CLOSE:
                        break
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug(f"读取循环异常: {e}")
        finally:
            self._closed = True
            self._connect_event.clear()
            for inv_id, future in self._pending.items():
                if not future.done():
                    future.set_exception(RuntimeError("连接已断开"))

    def _handle_completion(self, msg):
        """处理 JSON 协议的 Completion 消息"""
        inv_id = msg.get("invocationId", "")
        error = msg.get("error")
        result = msg.get("result")
        future = self._pending.pop(inv_id, None)
        if not future or future.done():
            return
        if error:
            future.set_exception(RuntimeError(error))
        else:
            future.set_result(result)

    async def close(self):
        self._closed = True
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()


# ─── 轻小说 API 封装 ──────────────────────────────────────────────────────


@dataclass
class ChapterInfo:
    id: int
    title: str
    sort_num: int


@dataclass
class BookInfo:
    id: int
    title: str
    author: str
    introduction: str
    cover: str
    chapters: list[ChapterInfo] = field(default_factory=list)
    views: int = 0
    favorite: int = 0
    last_updated: str = ""
    created_at: str = ""


@dataclass
class ChapterContent:
    title: str
    content: str  # HTML
    chapters_toc: list[str] = field(default_factory=list)
    font_url: str = ""


class LightNovelAPI:
    """轻小说 API 封装"""

    # 默认选项：请求 gzip 压缩响应
    DEFAULT_OPTIONS = {"UseGzip": True}

    def __init__(self, client: SignalRClient):
        self._client = client

    @staticmethod
    def _decode_response(result: Any) -> dict:
        """解码 SignalR 返回的 response 字段（base64 + gzip）"""
        if result is None:
            return {}
        if isinstance(result, dict):
            response = result.get("response") or result.get("Response")
            if response is None:
                return result
            if isinstance(response, str):
                import base64
                import gzip
                decoded = base64.b64decode(response)
                decompressed = gzip.decompress(decoded)
                return json.loads(decompressed)
            if isinstance(response, bytes):
                import gzip
                decompressed = gzip.decompress(response)
                return json.loads(decompressed)
        return result if isinstance(result, dict) else {}

    async def get_book_info(self, book_id: int) -> BookInfo:
        raw = await self._client.invoke("GetBookInfo", {"Id": book_id}, self.DEFAULT_OPTIONS)
        return self._parse_book_info(self._decode_response(raw))

    def _parse_book_info(self, raw: Any) -> BookInfo:
        book = raw.get("Book", raw) if isinstance(raw, dict) else raw
        author = book.get("Author") or book.get("Arthur") or ""
        if not author:
            user = book.get("User", {})
            if isinstance(user, dict):
                author = user.get("UserName", "")
        info = BookInfo(
            id=book.get("Id", 0),
            title=book.get("Title", ""),
            author=author,
            introduction=book.get("Introduction", ""),
            cover=book.get("Cover", ""),
            views=book.get("Views", 0),
            favorite=book.get("Favorite", 0),
            last_updated=str(book.get("LastUpdatedAt", "")),
            created_at=str(book.get("CreatedAt", "")),
        )
        chapters_raw = book.get("Chapter", [])
        if isinstance(chapters_raw, list):
            for idx, ch in enumerate(chapters_raw, 1):
                if isinstance(ch, dict):
                    info.chapters.append(ChapterInfo(
                        id=ch.get("Id", 0),
                        title=ch.get("Title", f"第{idx}章"),
                        sort_num=idx,
                    ))
                elif isinstance(ch, str):
                    info.chapters.append(ChapterInfo(id=idx, title=ch, sort_num=idx))
        return info

    async def get_chapter_content(self, book_id: int, sort_num: int, convert: Optional[str] = None) -> ChapterContent:
        params = {"Bid": book_id, "SortNum": sort_num}
        if convert:
            params["Convert"] = convert
        raw = await self._client.invoke("GetNovelContent", params, self.DEFAULT_OPTIONS)
        return self._parse_chapter_content(self._decode_response(raw))

    def _parse_chapter_content(self, raw: Any) -> ChapterContent:
        chapter = raw.get("Chapter", raw) if isinstance(raw, dict) else raw
        return ChapterContent(
            title=chapter.get("Title", ""),
            content=chapter.get("Content", ""),
            chapters_toc=chapter.get("Chapters", []),
            font_url=chapter.get("Font", ""),
        )

    async def search_books(self, keyword: str, page: int = 1, size: int = 24, order: str = "latest", exact: bool = False):
        search_key = f'"{keyword}"' if exact else keyword
        raw = await self._client.invoke("GetBookList", {
            "Page": page, "Size": size, "KeyWords": search_key,
            "Order": order, "IgnoreJapanese": False, "IgnoreAI": False,
        }, self.DEFAULT_OPTIONS)
        return self._decode_response(raw)

    async def get_rank(self, days: int = 7):
        raw = await self._client.invoke("GetRank", {"Days": days}, self.DEFAULT_OPTIONS)
        return self._decode_response(raw)


# ─── HTML 转纯文本工具 ────────────────────────────────────────────────────


def html_to_text(html: str) -> str:
    text = html
    for tag in ["p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"]:
        text = re.sub(rf"<{tag}[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(rf"</{tag}>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r'<img[^>]*src=["\']([^"\']+)["\'][^>]*>', r' [图片: \1] ', text)
    text = re.sub(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', r'\2 (\1)', text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    for e, c in [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"), ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'")]:
        text = text.replace(e, c)
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    return text.strip()


def html_to_markdown(html: str) -> str:
    text = html
    text = re.sub(r"<h1[^>]*>(.*?)</h1>", r"# \1\n", text, flags=re.DOTALL)
    text = re.sub(r"<h2[^>]*>(.*?)</h2>", r"## \1\n", text, flags=re.DOTALL)
    text = re.sub(r"<h3[^>]*>(.*?)</h3>", r"### \1\n", text, flags=re.DOTALL)
    text = re.sub(r'<img[^>]*src=["\']([^"\']+)["\'][^>]*alt=["\']([^"\']*)["\'][^>]*>', r'![\2](\1)', text)
    text = re.sub(r'<img[^>]*src=["\']([^"\']+)["\'][^>]*>', r'![](\1)', text)
    text = re.sub(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', r'[\2](\1)', text, flags=re.DOTALL)
    text = re.sub(r"<p[^>]*>(.*?)</p>", r"\1\n\n", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<strong>(.*?)</strong>", r"**\1**", text, flags=re.DOTALL)
    text = re.sub(r"<b>(.*?)</b>", r"**\1**", text, flags=re.DOTALL)
    text = re.sub(r"<em>(.*?)</em>", r"*\1*", text, flags=re.DOTALL)
    text = re.sub(r"<i>(.*?)</i>", r"*\1*", text, flags=re.DOTALL)
    text = re.sub(r"<code>(.*?)</code>", r"`\1`", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    for e, c in [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"), ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'")]:
        text = text.replace(e, c)
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    return text.strip()


# ─── EPUB 打包 ────────────────────────────────────────────────────────────

XHTML_HEAD = """<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>"""
XHTML_MID = """</title><link rel="stylesheet" type="text/css" href="style.css"/></head>
<body>
<h1>"""
XHTML_TAIL = """</h1>
"""
XHTML_END = """</body></html>"""


def _make_xhtml(title: str, body: str) -> str:
    return f"{XHTML_HEAD}{title}{XHTML_MID}{title}{XHTML_TAIL}{body}{XHTML_END}"


def _html_body(html: str) -> str:
    """Extract body content from HTML, stripping <h1> wrapper if present"""
    # Remove the h1 title that's already included in chapter content
    body = re.sub(r'<h1[^>]*>.*?</h1>', '', html, flags=re.DOTALL).strip()
    return body


def build_epub(book_dir: Path, info, chapters_dir: Path, font_available: bool, cover_data: Optional[bytes] = None):
    """将已下载的 HTML 章节打包为 EPUB 3 文件"""
    import zipfile

    safe_title = sanitize_filename(info.title)
    epub_path = book_dir / f"{safe_title}.epub"

    oebps = book_dir / "OEBPS"
    oebps.mkdir(exist_ok=True)

    # 复制字体
    font_file = book_dir / "font.woff2"
    if font_available and font_file.exists():
        import shutil
        shutil.copy(font_file, oebps / "font.woff2")

    # 封面图片
    cover_ext = "jpg"
    cover_in_epub = None
    if cover_data:
        cover_path = oebps / f"cover.{cover_ext}"
        cover_path.write_bytes(cover_data)
        cover_in_epub = f"cover.{cover_ext}"

    # 写 style.css
    css = ""
    if font_available:
        css += '@font-face{font-family:read;src:url(font.woff2);}\n'
    css += 'body{font-family:read,sans-serif;line-height:1.8;margin:1em}\n'
    css += 'h1{font-size:1.4em;text-align:center;margin:1em 0}\n'
    css += 'p{margin:0.5em 0;text-indent:2em}\n'
    css += 'img{max-width:100%;height:auto}\n'
    (oebps / "style.css").write_text(css, encoding="utf-8")

    # 章节 XHTML 文件
    chapter_files = []
    for ch in info.chapters:
        fn = f"{ch.sort_num:04d}_{sanitize_filename(ch.title) or f'第{ch.sort_num}章'}"
        html_path = chapters_dir / f"{fn}.html"
        if html_path.exists():
            raw = html_path.read_text(encoding="utf-8")
            body = _html_body(raw)
            xhtml = _make_xhtml(ch.title, body)
            xhtml_path = oebps / f"ch{ch.sort_num:04d}.xhtml"
            xhtml_path.write_text(xhtml, encoding="utf-8")
            chapter_files.append((ch, f"ch{ch.sort_num:04d}.xhtml"))

    # 封面 XHTML
    if cover_in_epub:
        cover_xhtml = _make_xhtml("封面",
            f'<div style="text-align:center"><img src="{cover_in_epub}" alt="封面"/></div>')
        (oebps / "cover.xhtml").write_text(cover_xhtml, encoding="utf-8")

    if not chapter_files:
        log.warning("EPUB: 无章节可打包")
        return

    # --- content.opf ---
    uid = f"ln-{info.id}-{int(time.time())}"
    opf_lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="book-id">',
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">',
        f'  <dc:identifier id="book-id">urn:uuid:{uid}</dc:identifier>',
        f'  <dc:title>{info.title}</dc:title>',
        f'  <dc:creator>{info.author}</dc:creator>',
        f'  <dc:language>zh-CN</dc:language>',
        f'  <meta property="dcterms:modified">{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}</meta>',
        '</metadata>',
        '<manifest>',
        '  <item id="style" href="style.css" media-type="text/css"/>',
    ]
    spine_lines = ['<spine>']

    if cover_in_epub:
        opf_lines.append(f'  <item id="cover-img" href="{cover_in_epub}" media-type="image/jpeg" properties="cover-image"/>')
        opf_lines.append('  <item id="cover-page" href="cover.xhtml" media-type="application/xhtml+xml"/>')
        spine_lines.append('  <itemref idref="cover-page" linear="yes"/>')

    if font_available and (oebps / "font.woff2").exists():
        opf_lines.append('  <item id="font" href="font.woff2" media-type="font/woff2"/>')

    # 章节条目
    for ch, fn in chapter_files:
        cid = f"ch{ch.sort_num:04d}"
        opf_lines.append(f'  <item id="{cid}" href="{fn}" media-type="application/xhtml+xml"/>')
        spine_lines.append(f'  <itemref idref="{cid}"/>')

    # nav.xhtml
    nav_lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<!DOCTYPE html>',
        '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">',
        '<head><title>目录</title></head>',
        '<body><nav epub:type="toc"><h1>目录</h1><ol>',
    ]
    for ch, _ in chapter_files:
        nav_lines.append(f'  <li><a href="ch{ch.sort_num:04d}.xhtml">{ch.title}</a></li>')
    nav_lines.append('</ol></nav></body></html>')
    (oebps / "nav.xhtml").write_text("\n".join(nav_lines), encoding="utf-8")

    opf_lines.append('  <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>')

    opf_lines.append('</manifest>')
    spine_lines.append('</spine>')
    opf_lines.extend(spine_lines)
    opf_lines.append('</package>')
    (oebps / "content.opf").write_text("\n".join(opf_lines), encoding="utf-8")

    # --- toc.ncx (EPUB 2 兼容) ---
    ncx_lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">',
        '<head><meta name="dtb:uid" content="urn:uuid:{uid}"/></head>',
        f'<docTitle><text>{info.title}</text></docTitle>',
        '<navMap>',
    ]
    for i, (ch, _) in enumerate(chapter_files, 1):
        ncx_lines.append(f'  <navPoint id="nav{i}" playOrder="{i}">')
        ncx_lines.append(f'    <navLabel><text>{ch.title}</text></navLabel>')
        ncx_lines.append(f'    <content src="ch{ch.sort_num:04d}.xhtml"/>')
        ncx_lines.append(f'  </navPoint>')
    ncx_lines.append('</navMap></ncx>')
    (oebps / "toc.ncx").write_text("\n".join(ncx_lines), encoding="utf-8")

    # --- META-INF/container.xml ---
    meta_dir = book_dir / "META-INF"
    meta_dir.mkdir(exist_ok=True)
    (meta_dir / "container.xml").write_text(
        '<?xml version="1.0"?>'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        '<rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>'
        '</container>'
    )

    # --- 打包为 ZIP ---
    epub_path.unlink(missing_ok=True)
    with zipfile.ZipFile(epub_path, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("mimetype", "application/epub+zip")
    with zipfile.ZipFile(epub_path, "a", zipfile.ZIP_DEFLATED) as zf:
        for f in meta_dir.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(book_dir))
        for f in oebps.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(book_dir))

    log.info(f"EPUB 已生成: {epub_path.resolve()}")


# ─── 实用工具 ──────────────────────────────────────────────────────────────


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "untitled"


def format_number(n: int) -> str:
    return f"{n:,}"


# ─── 连接与下载核心 ─────────────────────────────────────────────────────────


API_ENDPOINTS = [
    ("主服务器", f"{API_BASE}{HUB_PATH}"),
    ("Cloudflare", f"{CF_API_BASE}{HUB_PATH}"),
]


async def _connect(token: Optional[str] = None) -> SignalRClient:
    """连接 SignalR，自动切换服务器"""
    last_error = None
    for label, url in API_ENDPOINTS:
        try:
            client = SignalRClient(url, token=token)
            await client.connect(max_retries=1)
            log.info(f"已连接到 {label}")
            return client
        except Exception as e:
            last_error = e
            log.warning(f"  {label} 连接失败: {e}")
    raise last_error or RuntimeError("所有服务器均连接失败")


async def download_book(
    book_id: int,
    output_dir: str = "./downloads",
    fmt: str = "html",
    convert: Optional[str] = None,
    max_concurrent: int = 5,
    include_cover: bool = False,
    token: Optional[str] = None,
    api_url: Optional[str] = None,
):
    start_time = time.time()
    client = SignalRClient(api_url, token=token) if api_url else await _connect(token)

    async with client:
        api = LightNovelAPI(client)

        log.info("正在获取书籍信息...")
        try:
            info = await api.get_book_info(book_id)
        except Exception as e:
            log.error(f"获取书籍信息失败: {e}")
            return False

        if not info.title:
            log.error("未找到该书")
            return False

        log.info(f"书名: 《{info.title}》")
        log.info(f"作者: {info.author}")
        log.info(f"章节数: {len(info.chapters)}")
        log.info(f"点击: {format_number(info.views)}  收藏: {format_number(info.favorite)}")

        safe_title = sanitize_filename(info.title)
        book_dir = Path(output_dir) / f"{safe_title}_{book_id}"
        book_dir.mkdir(parents=True, exist_ok=True)
        chapters_dir = book_dir / "chapters"
        chapters_dir.mkdir(exist_ok=True)

        metadata = {
            "id": info.id, "title": info.title, "author": info.author,
            "introduction": info.introduction, "cover": info.cover,
            "views": info.views, "favorite": info.favorite,
            "last_updated": info.last_updated, "created_at": info.created_at,
            "chapters": [{"id": ch.id, "title": ch.title, "sort_num": ch.sort_num} for ch in info.chapters],
            "download_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(book_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        if include_cover and info.cover:
            try:
                async with httpx.AsyncClient(timeout=30) as hc:
                    resp = await hc.get(info.cover)
                    if resp.status_code == 200:
                        ext = info.cover.rsplit(".", 1)[-1].split("?")[0] if "." in info.cover else "jpg"
                        (book_dir / f"cover.{ext}").write_bytes(resp.content)
                        log.info("封面已下载")
            except Exception as e:
                log.warning(f"封面下载失败: {e}")

        # epub 内部用 html 下载章节，最后打包
        use_epub = fmt == "epub"
        internal_fmt = "html" if use_epub else fmt
        total = len(info.chapters)
        ext = ".html" if internal_fmt == "html" else ".md" if internal_fmt == "markdown" else ".txt"
        failed_chapters = []

        # 字体处理：从第一章获取字体 URL 并下载
        font_css = ""
        font_available = False
        font_warned = False
        if internal_fmt in ("text", "markdown") and any(True for _ in info.chapters):
            font_warned = True

        async def ensure_font(content: ChapterContent) -> str:
            """确保字体文件已下载，返回 CSS @font-face 字符串"""
            nonlocal font_css, font_warned, font_available
            if font_css:
                return font_css
            font_path = content.font_url
            if not font_path:
                return ""
            font_full_url = f"https://api.lightnovel.life{font_path}" if font_path.startswith("/") else font_path
            font_file = book_dir / "font.woff2"
            if not font_file.exists():
                try:
                    async with httpx.AsyncClient(timeout=30) as hc:
                        resp = await hc.get(font_full_url)
                        if resp.status_code == 200:
                            font_file.write_bytes(resp.content)
                            log.info(f"字体已下载 ({len(resp.content)} bytes)")
                except Exception as e:
                    log.warning(f"字体下载失败: {e}")
                    return ""
            if font_file.exists() and font_file.stat().st_size > 0:
                font_css = '@font-face{font-family:read;src:url(../font.woff2)}body{font-family:read,sans-serif}\n'
                font_available = True
            if font_warned and font_css:
                log.warning("正文使用加密字体，text/markdown 格式无法解码。建议使用 HTML 格式并在浏览器中查看。")
                font_warned = False
            elif font_warned and not font_css:
                font_warned = False
            return font_css

        log.info(f"开始下载 {total} 章 (格式: {fmt}, 并发: {max_concurrent})...")

        sem = asyncio.Semaphore(max_concurrent)
        font_lock = asyncio.Lock()

        async def download_one(chapter: ChapterInfo) -> bool:
            for attempt in range(1, 4):
                try:
                    async with sem:
                        content = await api.get_chapter_content(book_id, chapter.sort_num, convert)
                except Exception as e:
                    if attempt < 3:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    log.warning(f"  ✗ 第{chapter.sort_num}章「{chapter.title}」下载失败: {e}")
                    return False

                # 下载字体（仅第一次）
                chapter_font_css = ""
                if content.font_url:
                    async with font_lock:
                        chapter_font_css = await ensure_font(content)

                if internal_fmt == "text":
                    text = html_to_text(content.content)
                elif internal_fmt == "markdown":
                    text = f"# {content.title}\n\n{html_to_markdown(content.content)}"
                else:
                    # HTML: 嵌入字体 CSS
                    if chapter_font_css:
                        text = f"<style>{chapter_font_css}</style>\n{content.content}"
                    else:
                        text = content.content

                safe_ch = sanitize_filename(chapter.title) or f"第{chapter.sort_num}章"
                filepath = chapters_dir / f"{chapter.sort_num:04d}_{safe_ch}{ext}"
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(text)
                return True
            return False

        tasks = [download_one(ch) for ch in info.chapters]
        done = 0
        for coro in asyncio.as_completed(tasks):
            done += 1
            if not await coro:
                for ch in info.chapters:
                    if ch.sort_num == done:
                        failed_chapters.append(f"  #{ch.sort_num} {ch.title}")
                        break
            bar_len = 30
            filled = int(bar_len * done / total)
            sys.stdout.write(f"\r  进度: |{'█' * filled}{'░' * (bar_len - filled)}| {done}/{total} ({done/total*100:.0f}%)")
            sys.stdout.flush()
        sys.stdout.write("\n")

        # EPUB 打包
        if use_epub:
            log.info("正在生成 EPUB...")
            cover_bytes = None
            if info.cover:
                try:
                    async with httpx.AsyncClient(timeout=30) as hc:
                        resp = await hc.get(info.cover)
                        if resp.status_code == 200:
                            cover_bytes = resp.content
                except Exception:
                    pass
            build_epub(book_dir, info, chapters_dir, font_available, cover_bytes)

        # 目录
        toc = []
        if internal_fmt == "markdown":
            toc.append(f"# {info.title}\n\n作者: {info.author}\n\n")
            if info.introduction:
                toc.append(f"## 简介\n\n{html_to_text(info.introduction)}\n\n")
            toc.append("## 目录\n\n")
            for ch in info.chapters:
                fn = f"{ch.sort_num:04d}_{sanitize_filename(ch.title) or f'第{ch.sort_num}章'}{ext}"
                toc.append(f"- [{ch.title}](chapters/{fn})\n")
        else:
            toc.append(f"书名: {info.title}\n作者: {info.author}\n")
            if info.introduction:
                toc.append(f"简介: {html_to_text(info.introduction)}\n")
            toc.append(f"\n共 {total} 章\n{'=' * 40}\n\n")
            for ch in info.chapters:
                toc.append(f"  {ch.sort_num:4d}. {ch.title}\n")
        with open(book_dir / f"目录{ext}", "w", encoding="utf-8") as f:
            f.writelines(toc)

        # 合并文件
        if internal_fmt in ("text", "markdown"):
            combined = book_dir / f"{safe_title}{ext}"
            with open(combined, "w", encoding="utf-8") as f:
                f.write(f"# {info.title}\n\n" if fmt == "markdown" else f"{'='*40}\n{info.title}\n{'='*40}\n\n")
                f.write(f"作者: {info.author}\n\n")
                if info.introduction:
                    it = html_to_text(info.introduction)
                    f.write(f"简介: {it}\n\n" if fmt == "text" else f"> {it}\n\n")
                for ch in info.chapters:
                    fn = f"{ch.sort_num:04d}_{sanitize_filename(ch.title) or f'第{ch.sort_num}章'}{ext}"
                    fp = chapters_dir / fn
                    if fp.exists():
                        f.write(f"\n\n---\n\n" if fmt == "markdown" else f"\n\n{'='*40}\n")
                        f.write(fp.read_text(encoding="utf-8"))
                        f.write("\n\n")

        elapsed = time.time() - start_time
        success_count = total - len(failed_chapters)
        log.info(f"下载完成！成功: {success_count}/{total}, 用时: {elapsed:.1f}s")
        log.info(f"保存路径: {book_dir.resolve()}")
        if failed_chapters:
            log.warning(f"失败的章节 ({len(failed_chapters)}):")
            for fc in failed_chapters:
                log.warning(f"  {fc}")
        return len(failed_chapters) == 0


async def show_book_info(book_id: int, token: Optional[str] = None, api_url: Optional[str] = None):
    client = SignalRClient(api_url, token=token) if api_url else await _connect(token)
    async with client:
        api = LightNovelAPI(client)
        try:
            info = await api.get_book_info(book_id)
        except Exception as e:
            log.error(f"获取书籍信息失败: {e}")
            return
        if not info.title:
            log.error("未找到该书")
            return
        print(f"\n{'='*50}")
        print(f"  书名: 《{info.title}》")
        print(f"  作者: {info.author}")
        print(f"  点击: {format_number(info.views)}")
        print(f"  收藏: {format_number(info.favorite)}")
        print(f"  更新: {info.last_updated}")
        print(f"  创建: {info.created_at}")
        print(f"  封面: {info.cover or '无'}")
        if info.introduction:
            intro = html_to_text(info.introduction)
            print(f"\n  简介: {intro[:300]}{'…' if len(intro) > 300 else ''}")
        print(f"\n  章节列表 ({len(info.chapters)} 章):")
        for i, ch in enumerate(info.chapters, 1):
            print(f"    {i:4d}. {ch.title}")
            if i >= 80:
                print(f"    ... 还有 {len(info.chapters) - 80} 章")
                break
        print(f"{'='*50}\n")


async def search_books(keyword: str, page: int = 1, token: Optional[str] = None, api_url: Optional[str] = None):
    client = SignalRClient(api_url, token=token) if api_url else await _connect(token)
    async with client:
        api = LightNovelAPI(client)
        try:
            result = await api.search_books(keyword, page=page)
        except Exception as e:
            log.error(f"搜索失败: {e}")
            return
        data = result.get("Data", [])
        total_pages = result.get("TotalPages", 0)
        if not data:
            print(f"未找到与「{keyword}」相关的书籍")
            return
        print(f"\n搜索「{keyword}」结果 (第{page}/{total_pages}页):")
        print(f"{'='*50}")
        for book in data:
            title = book.get("Title", "未知")
            author = book.get("Author", "") or book.get("UserName", "")
            bid = book.get("Id", 0)
            views = book.get("Views", 0)
            print(f"  [{bid:6d}] {title}")
            print(f"         作者: {author}  点击: {format_number(views)}")
            print()
        print(f"共 {len(data)} 条结果\n")


# ─── 命令行入口 ────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="轻小说下载器 - LightNovelShelf Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --token TOKEN search "转生"
  %(prog)s --token TOKEN info 16834
  %(prog)s --token TOKEN download 16834
  %(prog)s --token TOKEN download 16834 -f html -c 10 --cover
        """,
    )

    parser.add_argument("--debug", action="store_true", help="开启调试日志")
    parser.add_argument("--token", help="登录令牌 (从浏览器获取的 RefreshToken)")

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    def add_common_args(p):
        p.add_argument("--api", help="API 服务器地址 (默认自动选择)")

    info_parser = subparsers.add_parser("info", help="查看书籍信息")
    add_common_args(info_parser)
    info_parser.add_argument("book_id", type=int, help="书籍 ID")

    dl_parser = subparsers.add_parser("download", help="下载全书")
    add_common_args(dl_parser)
    dl_parser.add_argument("book_id", type=int, help="书籍 ID")
    dl_parser.add_argument("-o", "--output", default="./downloads", help="输出目录")
    dl_parser.add_argument("-f", "--format", choices=["html", "text", "markdown", "epub"], default="html",
                           help="输出格式 (默认: html)")
    dl_parser.add_argument("--t2s", action="store_true", help="繁体转简体")
    dl_parser.add_argument("--s2t", action="store_true", help="简体转繁体")
    dl_parser.add_argument("-c", "--concurrent", type=int, default=5, help="并发数 (默认: 5)")
    dl_parser.add_argument("--cover", action="store_true", help="同时下载封面")

    search_parser = subparsers.add_parser("search", help="搜索书籍")
    add_common_args(search_parser)
    search_parser.add_argument("keyword", help="搜索关键词")
    search_parser.add_argument("-p", "--page", type=int, default=1, help="页码")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)
    elif args.command == "download":
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    convert = None
    if hasattr(args, "t2s") and args.t2s:
        convert = "t2s"
    if hasattr(args, "s2t") and args.s2t:
        convert = "s2t"

    # ── Token 处理 ──
    token = args.token
    if not token and args.command in ("download", "info", "search"):
        token = prompt_token()

    if token:
        log.info("正在获取 session token...")
        try:
            session_token = asyncio.run(token_to_session(token))
        except Exception as e:
            log.error(f"Token 无效: {e}")
            sys.exit(1)
    else:
        session_token = None

    try:
        if args.command == "info":
            asyncio.run(show_book_info(args.book_id, token=session_token, api_url=args.api))
        elif args.command == "download":
            success = asyncio.run(download_book(
                book_id=args.book_id, output_dir=args.output, fmt=args.format,
                convert=convert, max_concurrent=args.concurrent,
                include_cover=args.cover, token=session_token, api_url=args.api,
            ))
            sys.exit(0 if success else 1)
        elif args.command == "search":
            asyncio.run(search_books(args.keyword, page=args.page, token=session_token, api_url=args.api))
    except PermissionError as e:
        log.error(f"认证失败: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("\n用户中断")
        sys.exit(130)
    except Exception as e:
        log.error(f"错误: {e}", exc_info=True if args.debug else False)
        sys.exit(1)


if __name__ == "__main__":
    main()
