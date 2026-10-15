<script>
  import { onMount } from "svelte";
  import { scalar } from "./value.js";
  import { scanPage } from "./operator.js";
  import JsonDocument from "./JsonDocument.svelte";

  let { run } = $props();
  const pageSize = 40;
  const jsonLimit = 4 * 1024 * 1024;

  let pattern = $state("*");
  let cursors = $state(["0"]);
  let nextCursor = $state("0");
  let keys = $state([]);
  let keyError = $state("");
  let busy = $state(false);
  let loadingKeys = $state(false);
  let opening = $state("");
  let confirmDelete = $state(false);
  let expiry = $state("");

  let selected = $state(null);
  let kind = $state("");
  let ttl = $state("");
  let rows = $state([]);
  let total = $state(0);
  let page = $state(0);
  let hashCursor = $state("0");
  let hashBack = $state(["0"]);
  let hashNext = $state("0");
  let streamFrom = $state("-");
  let streamBack = $state(["-"]);
  let detailError = $state("");
  let detailNote = $state("");
  let member = $state("");
  let memberHit = $state("");
  let stringText = $state("");
  let jsonText = $state("");
  let jsonSize = $state(0);
  let jsonLarge = $state(false);

  let cursor = $derived(cursors[cursors.length - 1]);

  onMount(() => loadKeys("0", true));

  async function loadKeys(start, replace, history = cursors) {
    if (loadingKeys) return;
    loadingKeys = true;
    try {
      let cursor = String(start);
      const names = [];
      let guard = 0;

      while (names.length < pageSize && guard < 64) {
        guard += 1;
        const value = await run(["SCAN", cursor, "MATCH", pattern || "*", "COUNT", String(pageSize)]);
        const scanned = scanPage(value);
        const next = scanned.cursor;
        const page = scanned.keys;

        names.push(...page);

        if (next === cursor && cursor !== "0") throw new Error("SCAN did not advance. Retry from the start.");
        if (next === "0") {
          cursor = "0";
          break;
        }

        cursor = next;
      }

      keys = [...new Set(names)];
      nextCursor = cursor;
      cursors = replace ? [String(start)] : history;
      keyError = "";
    } catch (error) {
      keyError = error.message;
    } finally {
      loadingKeys = false;
    }
  }

  function search(event) {
    event.preventDefault();
    loadKeys("0", true);
  }

  function nextKeys() {
    loadKeys(nextCursor, false, [...cursors, nextCursor]);
  }

  function previousKeys() {
    const previous = cursors.slice(0, -1);
    loadKeys(previous[previous.length - 1] ?? "0", false, previous);
  }

  async function openKey(name) {
    if (busy) return;
    busy = true;
    opening = name;
    detailError = "";
    try {
      const nextKind = scalar(await run(["TYPE", name]));
      const nextTtl = scalar(await run(["TTL", name]));
      const value = await readValue(name, nextKind, 0, "0", "-");
      selected = name;
      kind = nextKind;
      ttl = nextTtl;
      confirmDelete = false;
      page = 0;
      hashCursor = "0";
      hashBack = ["0"];
      streamFrom = "-";
      streamBack = ["-"];
      member = "";
      memberHit = "";
      applyValue(value);
    } catch (error) {
      detailError = error.message;
    } finally {
      busy = false;
      opening = "";
    }
  }

  async function loadValue() {
    detailError = "";
    applyValue(await readValue(selected, kind, page, hashCursor, streamFrom));
  }

  function applyValue(value) {
    ({ total, rows, stringText, detailNote, hashNext, jsonText, jsonSize, jsonLarge } = value);
  }

  async function readValue(name, type, currentPage, currentHashCursor, currentStreamFrom) {
    const result = { total: 0, rows: [], stringText: "", detailNote: "", hashNext: "0", jsonText: "", jsonSize: 0, jsonLarge: false };
    if (type === "json") {
      result.jsonSize = Number(scalar(await run(["MEMORY", "USAGE", name]))) || 0;
      if (result.jsonSize > jsonLimit) {
        result.jsonLarge = true;
        result.detailNote = `This document uses about ${Math.round(result.jsonSize / 1048576)} MiB, so it is not loaded whole. Read a path below.`;
        return result;
      }
      result.jsonText = scalar(await run(["JSON.GET", name]));
      result.total = result.jsonText.length;
      return result;
    }
    if (type === "string") {
      result.stringText = scalar(await run(["GET", name]));
      result.total = result.stringText.length;
      return result;
    }
    if (type === "list") {
      result.total = Number(scalar(await run(["LLEN", name]))) || 0;
      const start = currentPage * pageSize;
      const value = await run(["LRANGE", name, String(start), String(start + pageSize - 1)]);
      result.rows = value?.kind === "array" ? value.items.map(scalar) : [];
      return result;
    }
    if (type === "hash") {
      result.total = Number(scalar(await run(["HLEN", name]))) || 0;
      const value = await run(["HSCAN", name, currentHashCursor, "COUNT", String(pageSize)]);
      const items = value?.kind === "array" ? value.items : [];
      result.hashNext = scalar(items[0]) || "0";
      const flat = items[1]?.kind === "array" ? items[1].items : [];
      for (let index = 0; index < flat.length; index += 2) {
        result.rows.push({ field: scalar(flat[index]), value: scalar(flat[index + 1]) });
      }
      return result;
    }
    if (type === "set") {
      result.total = Number(scalar(await run(["SCARD", name]))) || 0;
      if (result.total > 200) {
        result.detailNote = `${result.total} members. The server returns a set all at once, so this view checks one member at a time.`;
        return result;
      }
      const value = await run(["SMEMBERS", name]);
      const all = value?.kind === "array" ? value.items.map(scalar) : [];
      result.total = all.length;
      result.rows = all.slice(currentPage * pageSize, (currentPage + 1) * pageSize);
      return result;
    }
    if (type === "zset") {
      result.total = Number(scalar(await run(["ZCARD", name]))) || 0;
      const start = currentPage * pageSize;
      const value = await run(["ZRANGE", name, String(start), String(start + pageSize - 1), "WITHSCORES"]);
      const flat = value?.kind === "array" ? value.items : [];
      for (let index = 0; index < flat.length; index += 2) {
        result.rows.push({ field: scalar(flat[index]), value: scalar(flat[index + 1]) });
      }
      return result;
    }
    if (type === "stream") {
      result.total = Number(scalar(await run(["XLEN", name]))) || 0;
      const value = await run(["XRANGE", name, currentStreamFrom, "+", "COUNT", String(pageSize + 1)]);
      let entries = value?.kind === "array" ? value.items : [];
      if (currentStreamFrom !== "-" && entries.length && scalar(entries[0]?.items?.[0]) === currentStreamFrom) {
        entries = entries.slice(1);
      } else if (entries.length > pageSize) {
        entries = entries.slice(0, pageSize);
      }
      result.rows = entries.map((entry) => ({
        id: scalar(entry.items?.[0]),
        fields: pairFields(entry.items?.[1]),
      }));
      return result;
    }
    result.detailNote = type === "none" ? "This key is gone." : `No paged viewer for ${type}.`;
    return result;
  }

  function pairFields(value) {
    const flat = value?.kind === "array" ? value.items : [];
    const fields = [];
    for (let index = 0; index < flat.length; index += 2) {
      fields.push({ field: scalar(flat[index]), value: scalar(flat[index + 1]) });
    }
    return fields;
  }

  async function checkMember(event) {
    event.preventDefault();
    await action(async () => {
      memberHit = "";
      memberHit = scalar(await run(["SISMEMBER", selected, member]));
    });
  }

  async function removeKey() {
    await action(async () => {
      await run(["DEL", selected]);
      keys = keys.filter((key) => key !== selected);
      selected = null;
      confirmDelete = false;
    });
  }

  async function action(operation) {
    if (busy) return;
    busy = true;
    const pagination = { page, hashCursor, hashBack, hashNext, streamFrom, streamBack, rows, total };
    try {
      await operation();
      detailError = "";
    } catch (error) {
      ({ page, hashCursor, hashBack, hashNext, streamFrom, streamBack, rows, total } = pagination);
      detailError = error.message;
    } finally {
      busy = false;
    }
  }

  async function changeExpiry(event) {
    event.preventDefault();
    await action(async () => {
      await run(["EXPIRE", selected, expiry]);
      ttl = scalar(await run(["TTL", selected]));
    });
  }

  function ttlLabel(value) {
    if (value === "-1") return "no expiry";
    if (value === "-2") return "missing";
    if (value === "") return "expiry not loaded";
    return `${value}s`;
  }

  let stringSlice = $derived(stringText.slice(page * 2000, (page + 1) * 2000));
</script>

<div class="browser">
  <section class="panel key-pane">
    <div class="section-heading"><h2>Keys</h2><span class="badge">{keys.length} on page</span></div>
    <form class="find" onsubmit={search}>
      <input bind:value={pattern} aria-label="Key pattern" placeholder="*" />
      <button type="submit" disabled={loadingKeys}>{loadingKeys ? "Loading..." : "Find"}</button>
    </form>
    <p class="hint">Match a pattern, like <code>session:*</code>. SCAN walks the selected owners.</p>
    {#if keyError}<p class="error-box" role="alert">{keyError}</p>{/if}
    <ul class="key-list">
      {#each keys as name}
        <li>
          <button type="button" class:on={name === selected} disabled={loadingKeys} onclick={() => openKey(name)}><span class="key-glyph" aria-hidden="true">#</span><span>{name || "(empty key)"}</span><span aria-hidden="true">&rsaquo;</span></button>
        </li>
      {/each}
    </ul>
    {#if keys.length === 0 && !loadingKeys && !keyError}<p class="empty">No keys on this page. Try a different pattern or continue the scan.</p>{/if}
    <div class="pager">
      <button type="button" disabled={loadingKeys || cursors.length < 2} onclick={previousKeys}>Previous</button>
      <span>Page {cursors.length}</span>
      <button type="button" disabled={loadingKeys || nextCursor === "0"} onclick={nextKeys}>Next</button>
    </div>
  </section>
  <section class="panel detail" aria-busy={busy}>
    <p class="sr-only" role="status">{opening ? `Reading ${opening}...` : busy ? "Updating details..." : ""}</p>
    {#if detailError}<p class="error-box" role="alert">{detailError}</p>{/if}
    {#if selected === null}
      <div class="empty-state"><span class="empty-symbol" aria-hidden="true">#</span><h2>A closer look at your data</h2><p>Select a key to inspect its type, lifetime, and value. Nothing is changed by opening a key.</p></div>
    {:else}
      <header class="detail-head">
        <div>
          <p class="eyebrow">KEY DETAILS</p><h2 class="mono">{selected || "(empty key)"}</h2>
          <p class="muted">{kind || "Loading type"} · {ttlLabel(ttl)}{["list", "hash", "set", "zset", "stream"].includes(kind) ? ` · ${total.toLocaleString()} ${kind === "hash" ? "fields" : kind === "stream" ? "entries" : kind === "list" ? "items" : "members"} total` : ""}</p>
        </div>
        <div class="actions"><button type="button" disabled={busy} onclick={() => openKey(selected)}>Refresh</button><button class="danger" type="button" disabled={busy} onclick={() => (confirmDelete = !confirmDelete)}>Delete</button></div>
      </header>
      {#if confirmDelete}<div class="error-box"><p>Delete this key and its value? This cannot be undone.</p><div class="actions"><button class="danger" disabled={busy} onclick={removeKey}>Confirm delete</button><button disabled={busy} onclick={() => (confirmDelete = false)}>Cancel</button></div></div>{/if}
      {#if detailNote}<p class="muted">{detailNote}</p>{/if}
      <details class="expiry-editor"><summary>Manage expiry</summary><form class="find" onsubmit={changeExpiry}><label>Lifetime in seconds<input bind:value={expiry} type="number" min="1" step="1" required placeholder="3600" /></label><button disabled={busy}>Set expiry</button><button type="button" disabled={busy} onclick={() => action(async () => { await run(["PERSIST", selected]); ttl = scalar(await run(["TTL", selected])); })}>Remove expiry</button></form></details>
      {#if kind === "json"}
        {#key selected}
          <JsonDocument name={selected} text={jsonText} size={jsonSize} truncated={jsonLarge} {run} changed={() => openKey(selected)} />
        {/key}
      {:else if kind === "string"}
        <div class="result mono">{stringSlice || "(empty)"}</div>
        {#if total > 2000}
          <div class="pager">
            <button type="button" disabled={busy || page === 0} onclick={() => (page -= 1)}>Previous</button>
            <span>{page * 2000 + 1}–{Math.min(total, (page + 1) * 2000)} of {total} characters</span>
            <button type="button" disabled={busy || (page + 1) * 2000 >= total} onclick={() => (page += 1)}>Next</button>
          </div>
        {/if}
      {:else if kind === "set" && total > 200}
        <form class="find" onsubmit={checkMember}>
          <input bind:value={member} aria-label="Member" placeholder="member" />
          <button type="submit" disabled={busy}>Check</button>
        </form>
        {#if memberHit}<p>{memberHit === "1" ? "Member" : "Not a member"}</p>{/if}
      {:else if kind === "hash" || kind === "zset"}
        <table>
          <thead><tr><th>{kind === "zset" ? "Member" : "Field"}</th><th>{kind === "zset" ? "Score" : "Value"}</th></tr></thead>
          <tbody>
            {#each rows as row}
              <tr><td>{row.field}</td><td class="mono">{row.value || "(empty)"}</td></tr>
            {/each}
          </tbody>
        </table>
      {:else if kind === "stream"}
        {#each rows as entry}
          <article class="entry">
            <header>{entry.id}</header>
            <table>
              <tbody>
                {#each entry.fields as field}
                  <tr><td>{field.field}</td><td class="mono">{field.value || "(empty)"}</td></tr>
                {/each}
              </tbody>
            </table>
          </article>
        {/each}
      {:else if kind === "list" || kind === "set"}
        <table>
          <thead><tr><th></th><th>Value</th></tr></thead>
          <tbody>
            {#each rows as row, index}
              <tr><td class="idx">{page * pageSize + index + 1}</td><td class="mono">{row || "(empty)"}</td></tr>
            {/each}
          </tbody>
        </table>
      {/if}
      {#if kind === "list" || kind === "zset" || (kind === "set" && total <= 200)}
        <div class="pager">
          <button type="button" disabled={busy || page === 0} onclick={() => action(async () => { page -= 1; await loadValue(); })}>Previous</button>
          <span>{total} total</span>
          <button type="button" disabled={busy || (page + 1) * pageSize >= total} onclick={() => action(async () => { page += 1; await loadValue(); })}>Next</button>
        </div>
      {:else if kind === "hash"}
        <div class="pager">
          <button type="button" disabled={busy || hashBack.length < 2} onclick={() => action(async () => { hashBack = hashBack.slice(0, -1); hashCursor = hashBack[hashBack.length - 1]; await loadValue(); })}>Previous</button>
          <span>{rows.length} fields</span>
          <button type="button" disabled={busy || hashNext === "0"} onclick={() => action(async () => { hashBack = [...hashBack, hashNext]; hashCursor = hashNext; await loadValue(); })}>Next</button>
        </div>
      {:else if kind === "stream"}
        <div class="pager">
          <button type="button" disabled={busy || streamBack.length < 2} onclick={() => action(async () => { streamBack = streamBack.slice(0, -1); streamFrom = streamBack[streamBack.length - 1]; await loadValue(); })}>Previous</button>
          <span>{total} entries</span>
          <button type="button" disabled={busy || rows.length < pageSize} onclick={() => action(async () => { streamFrom = rows[rows.length - 1].id; streamBack = [...streamBack, streamFrom]; await loadValue(); })}>Next</button>
        </div>
      {/if}
    {/if}
  </section>
</div>
