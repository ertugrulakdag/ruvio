#!/usr/bin/env python3
"""Write the shared head links, top menu and footer into every page of site/.

The parts live in scripts/site_chrome/ (head.html, header.html, footer.html), so a menu
entry or a cache-busting `?v=` changes in one file. Run after editing a part:

    python3 scripts/site_chrome.py           # rewrite the pages
    python3 scripts/site_chrome.py --check   # fail when a page differs (scripts/check.sh)

The active menu entry is marked at load time by site/js/chrome.js from <body data-page>.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"
PARTS = Path(__file__).resolve().parent / "site_chrome"

# Each part replaces the block of a page that this pattern finds; every page has each block once.
BLOCKS = {
    "head.html": re.compile(r'  <link rel="preconnect".*?<link rel="icon"[^\n]*\n', re.S),
    "header.html": re.compile(r'  <header class="top">.*?</header>\n', re.S),
    "footer.html": re.compile(r'  <footer class="site-foot">.*?</footer>\n', re.S),
}


def render(text: str, parts: dict[str, str], page: str) -> str:
    for name, pattern in BLOCKS.items():
        matches = pattern.findall(text)

        if len(matches) != 1:
            raise SystemExit(f"{page}: expected one {name} block, found {len(matches)}")

        text = pattern.sub(lambda _: parts[name], text)

    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="report pages that differ; change nothing")
    args = parser.parse_args()
    parts = {name: (PARTS / name).read_text(encoding="utf-8") for name in BLOCKS}
    stale = []

    for page in sorted(SITE.glob("*.html")):
        text = page.read_text(encoding="utf-8")
        rendered = render(text, parts, page.name)

        if rendered != text:
            stale.append(page.name)

            if not args.check:
                page.write_text(rendered, encoding="utf-8")

    if args.check and stale:
        print(f"site pages differ from scripts/site_chrome/: {', '.join(stale)}", file=sys.stderr)
        print("run: python3 scripts/site_chrome.py", file=sys.stderr)
        return 1

    print(f"site chrome {'ok' if args.check else 'written'}: {len(list(SITE.glob('*.html')))} pages")
    return 0


if __name__ == "__main__":
    sys.exit(main())
