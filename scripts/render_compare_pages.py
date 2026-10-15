#!/usr/bin/env python3
"""Renders one redis_compare_features.py result into the Performance and Memory pages.

    python3 scripts/render_compare_pages.py site/data/mac-2026-10-14.json \
        --id mac-2026-10-14 --machine "an Apple M-series Mac" \
        --env "Apple M2 Pro, 12 cores, macOS 15; server and load generator share the machine."

Idempotent: each section sits between <!-- <id>:start --> and <!-- <id>:end --> markers and is
inserted right after the page lede (newest first). The JSON is the script's results.json, optionally
with an "environment" object (machine, pinning, notes) added next to it.
"""
import argparse, json, re, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def rate(v):
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}k" if v >= 10e3 else f"{v / 1e3:.1f}k" if v >= 1e3 else f"{v:.0f}"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def ratio_cell(r, low=0.9):
    cls = ' class="lag"' if r < low else ""
    return f"<td{cls}>{r:.2f}×</td>"


def mem_ratio_cell(r):
    # Memory: highlight rows where Ruvio uses more than Redis.
    cls = ' class="lag"' if r > 1.0 else ""
    return f"<td{cls}>{r:.2f}×</td>"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path)
    p.add_argument("--id", required=True, help="marker and anchor, e.g. mac-2026-10-14")
    p.add_argument("--machine", required=True, help="short name for headings, e.g. 'a 2-vCPU Linux server'")
    p.add_argument("--env", required=True, help="one sentence describing the hardware and placement")
    p.add_argument("--profile", default="release", help="cargo profile the server binary was built with")
    a = p.parse_args()

    D = json.loads(a.data.read_text())
    rows, mem = D["rows"], D["memory"]
    cfg = D["config"]
    date = D["date"]
    redis = D["redis_version"].split()[2].removeprefix("v=") if D["redis_version"].startswith("Redis server") else D["redis_version"]
    start, end = f"<!-- {a.id}:start -->", f"<!-- {a.id}:end -->"
    rid = lambda label: f"{a.id}-row-" + re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")

    env = (f"<strong>Environment:</strong> {esc(a.env)} Ruvio commit <code>{esc(D['build']['commit'])}</code> "
           f"(<code>cargo build --profile {esc(a.profile)}</code>), one shard, <code>memory</code> durability; Redis {esc(redis)}.")
    method = (f"<code>scripts/redis_compare_features.py</code>: <code>redis-benchmark</code>, {cfg['clients']} clients, "
              f"pipeline {cfg['pipeline']}, request counts scaled by {cfg['scale']}, {cfg['rounds']} rounds with alternating order, median requests/s.")
    link = f'<a href="data/{a.data.name}">All samples and environment (JSON)</a>'

    rs = [(r["ruvio"] / r["redis"], r["label"]) for r in rows if r["redis"]]
    below = sorted((x, l) for x, l in rs if x < 0.9)
    summary = (f"{sum(1 for x, _ in rs if x >= 0.97)} of {len(rs)} rows at or above 0.97×, geometric mean "
               f"{statistics.geometric_mean(x for x, _ in rs):.2f}×; best "
               + ", ".join(f"{esc(l)} {x:.2f}×" for x, l in sorted(rs, reverse=True)[:3]) + "; lowest "
               + ", ".join(f"{esc(l)} {x:.3f}×" for x, l in sorted(rs)[:3]) + "; below 0.90×: "
               + (", ".join(f"{esc(l)} {x:.3f}×" for x, l in below) or "none") + ".")

    speed = ['<div class="table-wrap"><table><thead><tr><th>Command</th><th>Ruvio req/s</th><th>Redis req/s</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for r in rows:
        speed.append(f"<tr id=\"{rid(r['label'])}\"><td>{esc(r['label'])}</td><td>{rate(r['ruvio'])}</td><td>{rate(r['redis'])}</td>{ratio_cell(r['ruvio'] / r['redis'])}</tr>")
    speed.append("</tbody></table></div>")

    def keys(label):
        return 1 if "geo points" in label else int(label.split()[0].replace(",", ""))

    def fmt(kib, n):
        b = kib * 1024 / n
        return f"{b / 1048576:.1f} MiB" if n == 1 else f"{b:.0f} B"

    mt = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Structure (bytes per key)</th><th colspan="3">RSS growth from loading</th><th colspan="3">Total RSS after loading</th></tr>'
          f'<tr><th>Ruvio</th><th>Redis {esc(redis)}</th><th>Ruvio / Redis</th><th>Ruvio</th><th>Redis {esc(redis)}</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for m in mem:
        n = keys(m["label"])
        r, d = m["ruvio"], m["redis"]
        mt.append(f"<tr><td>{esc(m['label'])}</td><td>{fmt(r['delta_kib'], n)}</td><td>{fmt(d['delta_kib'], n)}</td>{mem_ratio_cell(r['delta_kib'] / d['delta_kib'])}"
                  f"<td>{r['loaded_kib'] / 1024:.1f} MiB</td><td>{d['loaded_kib'] / 1024:.1f} MiB</td>{mem_ratio_cell(r['loaded_kib'] / d['loaded_kib'])}</tr>")
    mt.append(f"<tr><td>Idle RSS</td><td>{mem[0]['ruvio']['idle_kib'] / 1024:.1f} MiB</td><td>{mem[0]['redis']['idle_kib'] / 1024:.1f} MiB</td><td></td><td></td><td></td><td></td></tr>")
    mt.append("</tbody></table></div>")
    more_delta = ", ".join(esc(m["label"]) for m in mem if m["ruvio"]["delta_kib"] > m["redis"]["delta_kib"]) or "none"
    more_total = ", ".join(esc(m["label"]) for m in mem if m["ruvio"]["loaded_kib"] > m["redis"]["loaded_kib"]) or "none"

    perf = f"""{start}
  <h2 id="{a.id}">Command comparison against Redis {esc(redis)} on {esc(a.machine)} · {date}</h2>
  <p><strong>Goal:</strong> repeat the 60-command grid on the current code and keep it as a dated record next to the earlier runs. {env}</p>
  <p><strong>Method:</strong> {method}</p>
  <p><strong>Result:</strong> {summary} Ratios under 0.90× are highlighted.</p>
  {chr(10).join(speed)}
  <p><strong>Limits:</strong> {cfg['rounds']} rounds of a short run; single rows move by about ±10% between runs on this machine.</p>
  <p class="links">{link}</p>
{end}"""

    memory = f"""{start}
  <h2 id="{a.id}-memory">Small-structure memory against Redis {esc(redis)} on {esc(a.machine)} · {date}</h2>
  <p><strong>Goal:</strong> bytes per structure on the current code, as a dated record next to the earlier runs. {env}</p>
  <p><strong>Method:</strong> a fresh server per structure, load pipelined (batches of 200, replies checked), resident memory after loading minus resident memory before (&ldquo;growth&rdquo;), plus the total after loading, divided by the number of keys for the per-key columns.</p>
  {chr(10).join(mt)}
  <p><strong>Interpretation:</strong> rows where Ruvio's growth exceeds Redis's: {more_delta}. Rows where Ruvio's total exceeds Redis's: {more_total}. Ratios above 1.00× (Ruvio uses more) are highlighted.</p>
  <p><strong>Limit:</strong> one key shape per structure, one run per cell, resident memory rather than allocator accounting.</p>
  <p class="links">{link}</p>
{end}"""

    for page, section in (("site/testing-performance.html", perf), ("site/testing-memory.html", memory)):
        path = ROOT / page
        text = path.read_text()
        if start in text:
            text = re.sub(re.escape(start) + r".*?" + re.escape(end), lambda _: section, text, flags=re.S)
        else:
            i = text.index('<p class="lede">')
            j = text.index("</p>", i) + 4
            text = text[:j] + "\n  " + section + text[j:]
        path.write_text(text)
    print("ok")


if __name__ == "__main__":
    main()
