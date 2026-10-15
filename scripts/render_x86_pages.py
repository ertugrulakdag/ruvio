#!/usr/bin/env python3
"""Renders site/data/x86-linux-2026-10-10.json into the Performance and Memory pages.

Idempotent: the sections sit between <!-- x86-2026-10-10:start --> and <!-- ...:end --> markers.
    python3 scripts/render_x86_pages.py
"""
import json, re, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
D = json.loads((ROOT / "site/data/x86-linux-2026-10-10.json").read_text())
START, END = "<!-- x86-2026-10-10:start -->", "<!-- x86-2026-10-10:end -->"


def rate(v):
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}k"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def ratio_cell(r, low=0.9):
    cls = ' class="lag"' if r < low else ""
    return f"<td{cls}>{r:.2f}×</td>"


def final(side):
    return D["after_memory_changes"][side]["features"]


def rows_by_label(side):
    return {r["label"]: r for r in final(side)["rows"]}


def speed_table():
    native, docker = rows_by_label("native"), rows_by_label("docker")
    out = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Command</th><th colspan="3">Native process</th><th colspan="3">Docker containers</th></tr>'
           '<tr><th>Ruvio req/s</th><th>Redis req/s</th><th>Ruvio / Redis</th><th>Ruvio req/s</th><th>Redis req/s</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for label, n in native.items():
        d = docker[label]
        out.append(
            f"<tr><td>{esc(label)}</td><td>{rate(n['ruvio'])}</td><td>{rate(n['redis'])}</td>{ratio_cell(n['ruvio'] / n['redis'])}"
            f"<td>{rate(d['ruvio'])}</td><td>{rate(d['redis'])}</td>{ratio_cell(d['ruvio'] / d['redis'])}</tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def summary():
    lines = []
    for side, name in (("native", "native"), ("docker", "Docker")):
        rs = [(r["ruvio"] / r["redis"], r["label"]) for r in final(side)["rows"] if r["redis"]]
        ge = sum(1 for x, _ in rs if x >= 0.97)
        below = sorted((x, l) for x, l in rs if x < 0.9)
        best = sorted(rs, reverse=True)[:3]
        geo = statistics.geometric_mean(x for x, _ in rs)
        low = ", ".join(f"{esc(l)} {x:.2f}×" for x, l in below) or "none"
        top = ", ".join(f"{esc(l)} {x:.2f}×" for x, l in best)
        lines.append(f"{name}: {ge} of {len(rs)} rows at or above 0.97×, geometric mean {geo:.2f}×; best {top}; below 0.90×: {low}.")
    return " ".join(lines)


def conc_table():
    n, d = D["native"]["concurrency_durability"], D["docker"]["concurrency_durability"]
    out = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Scenario</th><th colspan="3">Native process</th><th colspan="3">Docker containers</th></tr>'
           '<tr><th>Ruvio req/s (p99)</th><th>Redis req/s (p99)</th><th>Ruvio / Redis</th><th>Ruvio req/s (p99)</th><th>Redis req/s (p99)</th><th>Ruvio / Redis</th></tr></thead><tbody>']
    for a, b in zip(n, d):
        def c(x):
            return f"{rate(x['rps'])} ({x['p99_ms']:.2f} ms)"
        out.append(
            f"<tr><td>{esc(a['label'])}</td><td>{c(a['ruvio'])}</td><td>{c(a['redis'])}</td>{ratio_cell(a['ruvio']['rps'] / a['redis']['rps'], 0.95)}"
            f"<td>{c(b['ruvio'])}</td><td>{c(b['redis'])}</td>{ratio_cell(b['ruvio']['rps'] / b['redis']['rps'], 0.95)}</tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def counters_table():
    rows = D["native"]["hardware_counters_per_request"]
    out = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Command</th><th colspan="2">Cycles</th><th colspan="2">Instructions</th><th colspan="2">L1i misses</th><th colspan="2">Branch misses</th></tr>'
           '<tr>' + "<th>Ruvio</th><th>Redis</th>" * 4 + "</tr></thead><tbody>"]
    for r in rows:
        a, b = r["ruvio"], r["redis"]
        if "cycles" not in a or "cycles" not in b:
            continue
        label = r["label"].replace("LPUSH+LPOP mix: ", "")
        out.append(
            f"<tr><td>{esc(label)}</td><td>{a['cycles']:.0f}</td><td>{b['cycles']:.0f}</td><td>{a['instructions']:.0f}</td><td>{b['instructions']:.0f}</td>"
            f"<td>{a['L1-icache-load-misses']:.1f}</td><td>{b['L1-icache-load-misses']:.1f}</td><td>{a['branch-misses']:.1f}</td><td>{b['branch-misses']:.1f}</td></tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


def counters_summary():
    rows = [r for r in D["native"]["hardware_counters_per_request"] if "cycles" in r["ruvio"] and "cycles" in r["redis"]]
    fewer_i = sum(1 for r in rows if r["ruvio"]["instructions"] < r["redis"]["instructions"])
    fewer_c = sum(1 for r in rows if r["ruvio"]["cycles"] < r["redis"]["cycles"])
    fewer_l = sum(1 for r in rows if r["ruvio"]["L1-icache-load-misses"] < r["redis"]["L1-icache-load-misses"])
    return len(rows), fewer_i, fewer_c, fewer_l


def mem_table():
    out = ['<div class="table-wrap"><table><thead><tr><th rowspan="2">Structure (bytes per key)</th><th colspan="4">Native process</th><th colspan="4">Docker containers</th></tr>'
           '<tr><th>Ruvio before</th><th>Ruvio after</th><th>Redis 8.10.2</th><th>after / Redis</th><th>Ruvio before</th><th>Ruvio after</th><th>Redis 8.10.2</th><th>after / Redis</th></tr></thead><tbody>']
    before = {s: {m["label"]: m for m in D[s]["features"]["memory"]} for s in ("native", "docker")}
    after = {s: {m["label"]: m for m in final(s)["memory"]} for s in ("native", "docker")}

    def keys(label):
        if "geo points" in label:
            return 1
        return int(label.split()[0].replace(",", ""))

    def fmt(kib, n):
        b = kib * 1024 / n
        return f"{b / 1048576:.1f} MiB" if n == 1 else f"{b:.0f} B"

    for label, m in after["native"].items():
        n = keys(label)
        cells = []
        for side in ("native", "docker"):
            bef, aft = before[side][label], after[side][label]
            r = aft["ruvio"]["delta_kib"] / aft["redis"]["delta_kib"]
            cells.append(f"<td>{fmt(bef['ruvio']['delta_kib'], n)}</td><td>{fmt(aft['ruvio']['delta_kib'], n)}</td><td>{fmt(aft['redis']['delta_kib'], n)}</td>" + ratio_cell(r, 1.0001))
        out.append(f"<tr><td>{esc(label)}</td>{''.join(cells)}</tr>")
    ia = next(iter(after["native"].values()))
    ib = next(iter(after["docker"].values()))
    out.append(f"<tr><td>Idle RSS</td><td></td><td>{ia['ruvio']['idle_kib'] / 1024:.1f} MiB</td><td>{ia['redis']['idle_kib'] / 1024:.1f} MiB</td><td></td>"
               f"<td></td><td>{ib['ruvio']['idle_kib'] / 1024:.1f} MiB</td><td>{ib['redis']['idle_kib'] / 1024:.1f} MiB</td><td></td></tr>")
    out.append("</tbody></table></div>")
    return "\n".join(out)


ENV = ("<strong>Environment:</strong> AMD EPYC 9354P (Zen 4), a KVM guest with 2 vCPU (L1i 64 KiB per core), 8 GB, Ubuntu 24.04, kernel 6.8; "
       "Ruvio release builds (LTO), the commit is named in each section; the container runs use an image built from the project's <code>Dockerfile</code> and the native runs the same source built with <code>cargo build --release</code>; "
       "Redis 8.10.2 built from source for the native runs (jemalloc 5.3.0) and the <code>redis:8.10.2</code> image for the Docker runs. "
       "One shard, <code>memory</code> durability unless stated. Server on cpu 1 (<code>taskset</code> or <code>--cpuset-cpus</code>), "
       "<code>redis-benchmark</code> 8.10.2 on cpu 0, Docker with <code>--network host</code>.")


def perf_section():
    n_rows, fi, fc, fl = counters_summary()
    return f"""{START}
  <h2>x86 Linux comparison against Redis 8.10.2 · native and Docker · 2026-10-10</h2>
  <p><strong>Goal:</strong> repeat the Redis comparison on Linux x86 with real hardware counters, the latest Redis, and both a native process and containers. {ENV}</p>
  <p><strong>Method:</strong> 60 commands from <code>scripts/redis_compare_features.py</code>: <code>redis-benchmark</code>, 50 clients, pipeline 16, 64-byte values, 300,000 requests per command, five rounds with alternating order, median requests/s. The throughput table and the memory "after" columns come from the final code of this day (<code>1291229</code>); the concurrency, durability and hardware-counter sections below were measured on an earlier build of the same day (<code>d99356e</code>) before the memory changes. The VM also hosted an unrelated idle container stack (load about 0), so the samples of one cell often differ by 10 to 25% (all samples are in the JSON); differences inside about ±10% are not conclusions. SETRANGE is the one row below Redis in both setups, but the hardware-counter run below shows Ruvio using fewer cycles for it, so treat it as unresolved.</p>
  <h3>Command throughput</h3>
  <p><strong>Result:</strong> {summary()} Ratios under 0.90× are highlighted.</p>
  {speed_table()}
  <h3>Concurrency and durability</h3>
  <p><strong>Method:</strong> <code>redis-benchmark -t set|get</code> or <code>HSET</code>, 64-byte values, 100,000 random keys, three alternating rounds, median req/s and p99. Unpipelined rows are client-bound on this 2-vCPU host (about 50k req/s), so they show the latency tail rather than server capacity. <code>always</code> is Ruvio <code>--durability always</code> against Redis <code>appendonly yes</code> with <code>appendfsync always</code>, on the VM's virtual disk.</p>
  {conc_table()}
  <p><strong>Interpretation:</strong> with pipelining and 200 clients Ruvio is about 1.05–1.2× Redis and has the lower p99. With <code>always</code> durability Ruvio is behind Redis (0.63–0.90× across the two runs); the cause has not been investigated. Highlighted cells are under 0.95×.</p>
  <h3>Hardware counters per request (native)</h3>
  <p><strong>Method:</strong> <code>perf stat -p &lt;server pid&gt;</code> over one million requests of each command (50 clients, pipeline 16; user plus kernel, so cycles include the network system calls). Cycles, instructions, L1 instruction-cache load misses and branch misses divided by requests.</p>
  {counters_table()}
  <p><strong>Interpretation:</strong> across the {n_rows} commands Ruvio executes fewer instructions in {fi}, fewer cycles in {fc} and fewer L1i misses in {fl}. L1i misses are 4–9 per request for both servers; the 145-versus-2 gap measured earlier with a simulated 32 KiB cache is not present on this 64 KiB-L1i core.</p>
  <p><strong>Limits:</strong> one VM, 2 vCPU, one CPU model (not Intel, not bare metal), one shard, 64-byte values, fixed keys, loopback, short runs (no soak, no tail-latency study at fixed offered load). Docker used host networking, so the container network path is not measured. Docker runs have no hardware counters. A passing comparison here is not a capacity guarantee.</p>
  <p class="links"><a href="data/x86-linux-2026-10-10.json">All samples and environment (JSON)</a> <a href="https://github.com/salihcantekin/ruvio/blob/main/scripts/render_x86_pages.py">How this section is generated</a></p>
{END}"""


def mem_section():
    return f"""{START}
  <h2>Small-structure memory against Redis 8.10.2 · native and Docker · 2026-10-10</h2>
  <p><strong>Goal:</strong> bytes per structure for the data types added since the first comparison. {ENV}</p>
  <p><strong>Method:</strong> a fresh server per structure, load pipelined (batches of 200, replies checked), RSS delta from <code>/proc/&lt;pid&gt;/status</code> after loading minus RSS before, divided by the number of keys. Each structure is loaded into a new process of each engine; Redis was not reused between rows. <strong>Before</strong> is <code>d99356e</code>, <strong>after</strong> is <code>1291229</code>, which adds slab records from 16 to 256 bytes for small lists, hashes, sets and sorted sets and pooled value buffers in allocator-like size classes (a 279-byte HyperLogLog used a 512-byte buffer before).</p>
  {mem_table()}
  <p><strong>Interpretation:</strong> highlighted cells are structures where Ruvio uses more memory than Redis 8.10.2 (none in this run). Redis 8.10.2 packs small collections more tightly than 7.0.15, which is why the <em>before</em> column was close to or above Redis for several rows. <strong>Limit:</strong> one key shape per structure, 5,000 to 100,000 keys, one run per cell, RSS rather than allocator accounting; the Docker numbers are the same processes inside containers, so a difference between native and Docker columns is allocator or page behavior, not a configuration change.</p>
  <p class="links"><a href="data/x86-linux-2026-10-10.json">All samples and environment (JSON)</a></p>
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
