#!/usr/bin/env python3
"""Renders site/data/x86-azure-2026-10-11.json into the Performance and Memory pages.

Idempotent: the sections sit between <!-- azure-2026-10-11:start --> and <!-- ...:end --> markers and are
inserted right after the page lede (newest first).
    python3 scripts/render_azure_pages.py
"""

import sys

# The section this script rendered is a dated record; rerunning it would rewrite the page
# from the frozen data file. Refuse unless asked to on purpose.
if __name__ == "__main__" and "--force" not in sys.argv[1:]:
    sys.exit(f"{__file__}: frozen record (x86-azure-2026-10-11); pass --force to re-render it on purpose")

import json, re, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
D = json.loads((ROOT / "site/data/x86-azure-2026-10-11.json").read_text())
START, END = "<!-- azure-2026-10-11:start -->", "<!-- azure-2026-10-11:end -->"


def rate(v):
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}k" if v >= 10e3 else f"{v / 1e3:.1f}k" if v >= 1e3 else f"{v:.0f}"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def row_id(label):
    return "azure-row-" + re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def ratio_cell(r, low=0.9):
    cls = ' class="lag"' if r < low else ""
    return f"<td{cls}>{r:.2f}×</td>"


ROWS = D["features"]["rows"]
MEM = D["features"]["memory"]
CONC = D["concurrency"]


def speed_table():
    out = ['<div class="table-wrap"><table><thead><tr><th>Command</th><th>Ruvio req/s</th><th>Redis req/s</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for r in ROWS:
        out.append(f"<tr id=\"{row_id(r['label'])}\"><td>{esc(r['label'])}</td><td>{rate(r['ruvio'])}</td><td>{rate(r['redis'])}</td>{ratio_cell(r['ruvio'] / r['redis'])}</tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def summary():
    rs = [(r["ruvio"] / r["redis"], r["label"]) for r in ROWS if r["redis"]]
    ge = sum(1 for x, _ in rs if x >= 0.97)
    below = sorted((x, l) for x, l in rs if x < 0.9)
    best = sorted(rs, reverse=True)[:3]
    worst = sorted(rs)[:3]
    geo = statistics.geometric_mean(x for x, _ in rs)
    low = ", ".join(f"{esc(l)} {x:.2f}×" for x, l in below) or "none"
    top = ", ".join(f"{esc(l)} {x:.2f}×" for x, l in best)
    bot = ", ".join(f"{esc(l)} {x:.2f}×" for x, l in worst)
    return (f"{ge} of {len(rs)} rows at or above 0.97×, geometric mean {geo:.2f}×; best {top}; lowest {bot}; below 0.90×: {low}.")


def conc_table():
    out = ['<div class="table-wrap"><table><thead><tr><th>Scenario</th><th>Ruvio shards</th><th>Ruvio req/s (p99)</th><th>Redis req/s (p99)</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for a in CONC:
        def c(x):
            return f"{rate(x['rps'])} ({x['p99_ms']:.2f} ms)"
        out.append(f"<tr id=\"{row_id(a['label'])}\"><td>{esc(a['label'])}</td><td>{a['ruvio_shards']}</td><td>{c(a['ruvio'])}</td><td>{c(a['redis'])}</td>{ratio_cell(a['ruvio']['rps'] / a['redis']['rps'], 0.95)}</tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def mem_table():
    out = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Structure (bytes per key)</th><th colspan="3">RSS growth from loading</th><th colspan="3">Total RSS after loading</th></tr>'
           '<tr><th>Ruvio</th><th>Redis 8.10.2</th><th>Ruvio / Redis</th><th>Ruvio</th><th>Redis 8.10.2</th><th>Ruvio / Redis</th></tr></thead><tbody>']

    def keys(label):
        return 1 if "geo points" in label else int(label.split()[0].replace(",", ""))

    def fmt(kib, n):
        b = kib * 1024 / n
        return f"{b / 1048576:.1f} MiB" if n == 1 else f"{b:.0f} B"

    for m in MEM:
        n = keys(m["label"])
        r, d = m["ruvio"], m["redis"]
        out.append(
            f"<tr><td>{esc(m['label'])}</td><td>{fmt(r['delta_kib'], n)}</td><td>{fmt(d['delta_kib'], n)}</td>{ratio_cell(r['delta_kib'] / d['delta_kib'], 1.0001)}"
            f"<td>{r['loaded_kib'] / 1024:.1f} MiB</td><td>{d['loaded_kib'] / 1024:.1f} MiB</td>{ratio_cell(r['loaded_kib'] / d['loaded_kib'], 1.0001)}</tr>")
    a = MEM[0]
    out.append(f"<tr><td>Idle RSS</td><td>{a['ruvio']['idle_kib'] / 1024:.1f} MiB</td><td>{a['redis']['idle_kib'] / 1024:.1f} MiB</td><td></td><td></td><td></td><td></td></tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def mem_summary():
    more_delta = [m["label"] for m in MEM if m["ruvio"]["delta_kib"] > m["redis"]["delta_kib"]]
    more_total = [m["label"] for m in MEM if m["ruvio"]["loaded_kib"] > m["redis"]["loaded_kib"]]
    return more_delta, more_total


ENV = (f"<strong>Environment:</strong> {D['host']['description']} "
       f"Ruvio commit <code>{D['build']['ruvio_commit']}</code> (<code>cargo build --release</code>); {D['build']['redis']}")


def perf_section():
    return f"""{START}
  <h2 id="azure-2026-10-11">Linux x86 comparison against Redis 8.10.2 on a 4-core VM · 2026-10-11</h2>
  <p><strong>Goal:</strong> repeat the speed comparison on a Linux machine with enough cores to keep server and load generator apart, with a fast data disk for the durability rows, and measure what changed in the WAL and the cross-shard path since the last run. {ENV}</p>
  <p><strong>Method:</strong> {D['method']['features']} Concurrency, durability and multi-shard rows: <code>scripts/bench_concurrency_durability.py</code>, {D['method']['concurrency']}</p>
  <h3 id="azure-commands">Command throughput</h3>
  <p><strong>Result:</strong> {summary()} Ratios under 0.90× are highlighted (none).</p>
  {speed_table()}
  <h3 id="azure-durability">Concurrency, durability and Ruvio shards</h3>
  <p><strong>Result:</strong> {D['notes']['concurrency']}</p>
  {conc_table()}
  <p><strong>Limits:</strong> {D['notes']['limits']}</p>
  <p class="links"><a href="data/x86-azure-2026-10-11.json">All samples and environment (JSON)</a> <a href="https://github.com/salihcantekin/ruvio/blob/main/scripts/render_azure_pages.py">How this section is generated</a></p>
{END}"""


def mem_section():
    more_delta, more_total = mem_summary()
    hl = ", ".join(esc(x) for x in more_delta) or "none"
    ht = ", ".join(esc(x) for x in more_total) or "none"
    return f"""{START}
  <h2>Small-structure memory against Redis 8.10.2 on a 4-core VM · 2026-10-11</h2>
  <p><strong>Goal:</strong> bytes per structure on a second Linux machine, after the slab and size-class work. {ENV}</p>
  <p><strong>Method:</strong> {D['method']['memory']}</p>
  {mem_table()}
  <p><strong>Interpretation:</strong> {D['notes']['memory']} Rows where Ruvio's growth exceeds Redis's: {hl}. Rows where Ruvio's total RSS exceeds Redis's: {ht}.</p>
  <p><strong>Limit:</strong> one key shape per structure, 5,000 to 100,000 keys, one run per cell, RSS rather than allocator accounting, native process only (no Docker run on this machine).</p>
  <p class="links"><a href="data/x86-azure-2026-10-11.json">All samples and environment (JSON)</a></p>
{END}"""


def patch(path, section, marker):
    text = path.read_text()
    if START in text:
        text = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda m: section, text, flags=re.S)
    else:
        i = text.index(marker)
        j = text.index("</p>", i) + 4
        text = text[: j] + "\n  " + section + text[j:]
    path.write_text(text)


patch(ROOT / "site/testing-performance.html", perf_section(), '<p class="lede">')
patch(ROOT / "site/testing-memory.html", mem_section(), '<p class="lede">')
print("ok")
