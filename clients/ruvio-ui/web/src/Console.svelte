<script>
  import { tick } from "svelte";
  import Icon from "./Icon.svelte";
  import { tokenize, classify, explainError, formatValue, viewsFor, LIMITS } from "./console.js";

  let { run, scopeLabel = "", endpoint = "" } = $props();

  const examples = [
    { line: "PING", note: "Check the connection" },
    { line: "INFO", note: "Server statistics" },
    { line: "SET greeting hello", note: "Write a string" },
    { line: "GET greeting", note: "Read it back" },
    { line: "SCAN 0 COUNT 20", note: "List keys, a page at a time" },
    { line: "TYPE greeting", note: "Find out what a key holds" },
    { line: "TTL greeting", note: "Seconds until it expires" },
    { line: "DBSIZE", note: "Number of keys" },
  ];

  let connected = $state(false);
  let showInfo = $state(false);
  let entries = $state([]);
  let input = $state("");
  let busy = $state(false);
  let fullscreen = $state(false);
  let copied = $state(0);
  let pending = $state(null);
  let history = [];
  let cursor = -1;
  let draft = "";
  let nextId = 1;
  let screen = $state();
  let field = $state();

  function connect() {
    connected = true;
    tick().then(() => field?.focus());
  }

  function disconnect() {
    connected = false;
    entries = [];
    pending = null;
    input = "";
    history = [];
    cursor = -1;
  }

  async function scrollEnd() {
    await tick();
    if (screen) screen.scrollTop = screen.scrollHeight;
  }

  function add(entry) {
    const item = { id: nextId++, limit: LIMITS.lines, view: "auto", trim: false, ...entry };
    entries = [...entries, item].slice(-100);
    scrollEnd();
    return item.id;
  }

  async function execute(line, args) {
    busy = true;
    const started = performance.now();
    try {
      const value = await run(args);
      add({ line, kind: "reply", value, ms: performance.now() - started, scope: scopeLabel });
    } catch (error) {
      add({ line, kind: "error", ...explainError(error.message), ms: performance.now() - started, scope: scopeLabel });
    } finally {
      busy = false;
      await tick();
      field?.focus();
    }
  }

  async function submit(event) {
    event?.preventDefault();
    const line = input.trim();
    if (!line || busy) return;
    input = "";
    cursor = -1;
    if (history[history.length - 1] !== line) history = [...history, line].slice(-100);
    pending = null;
    const parsed = tokenize(line);
    if (parsed.error) return add({ line, kind: "notice", level: "error", text: parsed.error });
    if (!parsed.args.length) return;
    const verdict = classify(parsed.args);
    if (verdict.kind === "blocked") return add({ line, kind: "notice", level: "error", text: `Not run. ${verdict.reason}` });
    if (verdict.kind === "confirm") {
      pending = { line, args: parsed.args, reason: verdict.reason };
      return;
    }
    await execute(line, parsed.args);
  }

  async function confirm() {
    const item = pending;
    pending = null;
    if (item) await execute(item.line, item.args);
  }

  function keydown(event) {
    if (event.key === "ArrowUp" && history.length) {
      event.preventDefault();
      if (cursor === -1) draft = input;
      cursor = Math.min(history.length - 1, cursor + 1);
      input = history[history.length - 1 - cursor];
    } else if (event.key === "ArrowDown" && cursor >= 0) {
      event.preventDefault();
      cursor -= 1;
      input = cursor === -1 ? draft : history[history.length - 1 - cursor];
    } else if (event.key === "Escape") {
      if (!input && !pending && fullscreen) fullscreen = false;
      input = "";
      pending = null;
      cursor = -1;
    } else if (event.key.toLowerCase() === "l" && event.ctrlKey) {
      event.preventDefault();
      entries = [];
    }
  }

  function tryExample(line) {
    input = line;
    field?.focus();
  }

  function more(entry) {
    entry.limit = Math.min(LIMITS.maxLines, entry.limit + LIMITS.step);
  }

  // Full result in the entry's current view: no line or string caps. Built only when asked for.
  function fullText(entry) {
    const { lines } = formatValue(entry.value, { limit: Infinity, string: Infinity, view: entry.view, trim: entry.trim });
    return lines.map((item) => item.text).join("\n");
  }

  async function copy(entry) {
    try {
      await navigator.clipboard.writeText(fullText(entry));
      copied = entry.id;
      setTimeout(() => { if (copied === entry.id) copied = 0; }, 1500);
    } catch {
      /* clipboard can be unavailable; use Download instead */
    }
  }

  function download(entry) {
    const out = formatValue(entry.value, { limit: 1, view: entry.view });
    const ext = out.mode === "json" ? "json" : "txt";
    const url = URL.createObjectURL(new Blob([fullText(entry)], { type: "text/plain" }));
    const link = Object.assign(document.createElement("a"), { href: url, download: `ruvio-result.${ext}` });
    link.click();
    URL.revokeObjectURL(url);
  }
</script>

<section class="console-page" class:fullscreen aria-label="Command console">
  {#if !connected}
    <div class="panel console-gate">
      <span class="console-gate-icon"><Icon name="terminal" /></span>
      <h2>Command console</h2>
      <p>Run Ruvio commands directly against {endpoint || "the server"}. The console stays closed until you connect, and commands run with the permissions of the user you signed in with.</p>
      <button class="primary" onclick={connect}>Connect console</button>
      {@render info()}
    </div>
  {:else}
    <div class="panel console-box">
      <header class="console-bar">
        <span class="status-dot"></span>
        <span class="mono">{endpoint}</span>
        <span class="badge">{scopeLabel || "All shards"}</span>
        <span class="console-bar-actions">
          <button class="quiet" onclick={() => (showInfo = !showInfo)} aria-expanded={showInfo}><Icon name="info" /> Help</button>
          <button class="quiet icon-only" onclick={() => (fullscreen = !fullscreen)} aria-pressed={fullscreen} aria-label={fullscreen ? "Exit full screen" : "Full screen"} title={fullscreen ? "Exit full screen (Esc)" : "Full screen"}><Icon name={fullscreen ? "shrink" : "expand"} /></button>
          <button class="quiet" onclick={() => (entries = [])} disabled={!entries.length}>Clear</button>
          <button class="quiet" onclick={disconnect}>Disconnect</button>
        </span>
      </header>
      {#if showInfo}<div class="console-info">{@render info()}</div>{/if}
      <div class="console-screen" bind:this={screen} aria-live="polite">
        {#if !entries.length}<p class="muted console-empty">Type a command below and press Enter.</p>{/if}
        {#each entries as entry (entry.id)}
          <article class="console-entry">
            <header class="console-head">
              <p class="console-line"><span aria-hidden="true">&rsaquo;</span> <span class="mono">{entry.line}</span></p>
              {#if entry.ms != null}<small class="muted console-meta">{Math.round(entry.ms)} ms{entry.scope ? ` / ${entry.scope}` : ""}</small>{/if}
            </header>
            {#if entry.kind === "reply"}
              {@const out = formatValue(entry.value, { limit: entry.limit, view: entry.view, trim: entry.trim })}
              {@const views = viewsFor(entry.value)}
              <div class="console-tools">
                {#if out.mode !== "quoted" || views.length > 3 || entry.view !== "auto"}
                  <span class="seg" role="group" aria-label="Result view">
                    {#each views as option}<button type="button" class:on={entry.view === option.id} aria-pressed={entry.view === option.id} onclick={() => (entry.view = option.id)}>{option.label}</button>{/each}
                  </span>
                {/if}
                {#if out.mode !== "quoted"}
                  <button type="button" class="chip" class:on={entry.trim} aria-pressed={entry.trim} onclick={() => (entry.trim = !entry.trim)} title="Trim whitespace and drop blank lines">Trim</button>
                {/if}
                <span class="console-tools-end">
                  <button type="button" class="chip" onclick={() => copy(entry)}>{copied === entry.id ? "Copied" : "Copy all"}</button>
                  <button type="button" class="chip" onclick={() => download(entry)}>Download</button>
                </span>
              </div>
              <pre class="console-out">{#each out.lines as item}<span class={`t-${item.type}`}>{item.text}</span>{"\n"}{/each}</pre>
              <p class="console-foot muted">
                {#if out.total > 1}<span>{out.total.toLocaleString()} items</span>{/if}
                {#if out.more}
                  <span>Showing the first {out.lines.length.toLocaleString()} lines.</span>
                  {#if entry.limit < LIMITS.maxLines}<button class="text-button" onclick={() => more(entry)}>Show {Math.min(LIMITS.step, LIMITS.maxLines - entry.limit)} more</button>{:else}<span>Limit reached. Copy all or Download for the rest, or narrow the request.</span>{/if}
                {/if}
              </p>
            {:else if entry.kind === "error"}
              <div class="console-error" role="alert"><strong>{entry.text}</strong>{#if entry.hint}<p>{entry.hint}</p>{/if}</div>
            {:else}
              <div class="console-error" class:warn={entry.level !== "error"} role="alert"><strong>{entry.text}</strong></div>
            {/if}
          </article>
        {/each}
        {#if busy}<p class="muted">Running...</p>{/if}
      </div>
      {#if pending}
        <div class="console-confirm" role="alertdialog" aria-label="Confirm command">
          <p><strong>{pending.reason}</strong> Run <code>{pending.line}</code>?</p>
          <div class="actions"><button class="danger" onclick={confirm}>Run anyway</button><button onclick={() => (pending = null)}>Cancel</button></div>
        </div>
      {/if}
      <form class="console-input" onsubmit={submit}>
        <span aria-hidden="true">&rsaquo;</span>
        <input bind:this={field} bind:value={input} onkeydown={keydown} disabled={busy} autocomplete="off" autocapitalize="off" spellcheck="false" aria-label="Command" placeholder="GET mykey" />
        <button class="primary" type="submit" disabled={busy || !input.trim()}>Run</button>
      </form>
    </div>
  {/if}
</section>

{#snippet info()}
  <div class="console-help">
    <div>
      <h3>What you can do</h3>
      <ul>
        <li>Read and write data: strings, hashes, lists, sets, sorted sets, streams, JSON.</li>
        <li>Inspect the server: <code>INFO</code>, <code>DBSIZE</code>, <code>SCAN</code>, <code>TTL</code>, <code>TYPE</code>.</li>
        <li>Manage access and settings with the matching commands, within your ACL permissions.</li>
      </ul>
      <h3>Good to know</h3>
      <ul>
        <li>Quote values with spaces: <code>SET note "hello world"</code>.</li>
        <li>Large replies are cut to {LIMITS.lines} lines and long values to {LIMITS.string.toLocaleString()} characters. You can expand, or copy the full result.</li>
        <li>Prefer <code>SCAN</code> with <code>COUNT</code> over <code>KEYS</code> on big data.</li>
        <li>Destructive commands (<code>FLUSHALL</code>, <code>SHUTDOWN</code>, <code>CONFIG SET</code>, ...) ask before running.</li>
        <li>Streaming and blocking commands (<code>SUBSCRIBE</code>, <code>MONITOR</code>, <code>BLPOP</code>, <code>MULTI</code>) are not supported here. Pub/Sub has its own tab.</li>
        <li>Keys: Up and Down browse history, Esc clears the line, Ctrl+L clears the screen. Commands also appear in Session activity.</li>
      </ul>
    </div>
    <div>
      <h3>Try one</h3>
      <ul class="console-examples">
        {#each examples as item}
          <li><button class="text-button mono" onclick={() => tryExample(item.line)} disabled={!connected}>{item.line}</button><span class="muted"> {item.note}</span></li>
        {/each}
      </ul>
      {#if !connected}<p class="hint">Connect first, then click an example to fill the command line.</p>{/if}
    </div>
  </div>
{/snippet}
