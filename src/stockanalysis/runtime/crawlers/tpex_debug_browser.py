"""Browser setup used by the explicit TPEX diagnostic runtime."""

import json
import logging
import os
import shutil
import sys
import tempfile

from DrissionPage import ChromiumOptions, ChromiumPage


MANIFEST_CONTENT = {
    "manifest_version": 3,
    "name": "Turnstile Patcher",
    "version": "0.1",
    "content_scripts": [{
        "js": ["./script.js"],
        "matches": ["<all_urls>"],
        "run_at": "document_start",
        "all_frames": True,
        "world": "MAIN",
    }],
}

SCRIPT_CONTENT = """
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
let screenX = getRandomInt(800, 1200);
let screenY = getRandomInt(400, 600);
Object.defineProperty(MouseEvent.prototype, 'screenX', { value: screenX });
Object.defineProperty(MouseEvent.prototype, 'screenY', { value: screenY });

Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-TW', 'zh'] });
Object.defineProperty(navigator, 'platform', { get: () => 'Linux x86_64' });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 1 });
"""


def _create_extension() -> str:
    temp_dir = tempfile.mkdtemp(prefix="turnstile_extension_")
    try:
        manifest_path = os.path.join(temp_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as manifest_file:
            json.dump(MANIFEST_CONTENT, manifest_file, indent=4)

        script_path = os.path.join(temp_dir, "script.js")
        with open(script_path, "w", encoding="utf-8") as script_file:
            script_file.write(SCRIPT_CONTENT.strip())
        return temp_dir
    except Exception:
        _cleanup_extension(temp_dir)
        raise


def _cleanup_extension(path: str) -> None:
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
    except Exception as exc:
        logging.warning("Failed to clean up browser extension %s: %s", path, exc)


def get_patched_browser(
    options: ChromiumOptions | None = None,
    headless: bool = True,
) -> ChromiumPage:
    platform_id = "Windows NT 10.0; Win64; x64"
    if sys.platform in {"linux", "linux2"}:
        platform_id = "X11; Linux x86_64"
    elif sys.platform == "darwin":
        platform_id = "Macintosh; Intel Mac OS X 10_15_7"
    user_agent = (
        f"Mozilla/5.0 ({platform_id}) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0.7103.113 Safari/537.36"
    )

    if options is None:
        options = ChromiumOptions().auto_port()
    if headless:
        options.headless(True)

    options.set_user_agent(user_agent)
    options.set_argument("--disable-dev-shm-usage")
    options.set_argument("--disable-gpu")
    options.set_argument("--no-sandbox")
    options.set_argument("--lang=zh-TW")
    options.set_argument("--intl.accept_languages=zh-TW,zh")
    options.set_argument("--window-size=1280,800")
    options.set_argument("--start-maximized")
    options.set_argument("--disable-blink-features=AutomationControlled")

    if "--blink-settings=imagesEnabled=false" in options._arguments:
        raise RuntimeError("To bypass Turnstile, imagesEnabled must be True")
    if "--incognito" in options._arguments:
        raise RuntimeError(
            "Cannot bypass Turnstile in incognito mode. Please run in normal browser mode."
        )

    extension_path = _create_extension()
    try:
        options.add_extension(extension_path)
        page = ChromiumPage(options)
        page.run_js(
            "Object.defineProperty(navigator, 'languages', "
            "{ get: () => ['zh-TW', 'zh'] });"
        )
        logging.info("[Debug] page UA: %s", page.run_js("return navigator.userAgent"))
        return page
    finally:
        _cleanup_extension(extension_path)
