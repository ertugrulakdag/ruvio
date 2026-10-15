<script>
  import ValueView from "./ValueView.svelte";
  import { present, scalar } from "./value.js";
  import JsonTree from "./JsonTree.svelte";
  import { pretty } from "./json.js";

  let { value, command = "", mode = "both" } = $props();
  const pageSize = 40;
  let page = $state(0);

  let view = $derived(present(command, value, mode));

  $effect(() => {
    value;
    command;
    page = 0;
  });

  function slice(rows) {
    const start = page * pageSize;
    return {
      rows: rows.slice(start, start + pageSize),
      total: rows.length,
      start,
    };
  }
</script>

{#snippet pager(total)}
  {#if total > pageSize}
    <div class="pager">
      <button type="button" disabled={page === 0} onclick={() => (page -= 1)}>Previous</button>
      <span>{page * pageSize + 1}–{Math.min(total, (page + 1) * pageSize)} of {total}</span>
      <button type="button" disabled={(page + 1) * pageSize >= total} onclick={() => (page += 1)}>Next</button>
    </div>
  {/if}
{/snippet}

{#snippet entries(list)}
  {@const window = slice(list)}
  {#each window.rows as entry}
    <article class="entry">
      <header>{entry.id}</header>
      {#if entry.fields.length === 0}
        <p class="muted">No fields</p>
      {:else}
        <table>
          <thead><tr><th>Field</th><th>Value</th></tr></thead>
          <tbody>
            {#each entry.fields as field}
              <tr>
                <td>{field.field}</td>
                <td class="mono">{field.value || "(empty)"}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </article>
  {/each}
  {@render pager(window.total)}
{/snippet}

<div class="structured">
  {#if ["list", "pairs", "scan"].includes(view.type)}
    <p class="hint">{view.rows.length.toLocaleString()} items in this reply</p>
  {:else if view.type === "entries"}
    <p class="hint">{view.entries.length.toLocaleString()} entries in this reply</p>
  {:else if view.type === "streams"}
    <p class="hint">{view.streams.length.toLocaleString()} streams / {view.streams.reduce((sum, stream) => sum + stream.entries.length, 0).toLocaleString()} entries in this reply</p>
  {:else if view.type === "json"}
    <p class="hint">JSON document</p>
  {:else if view.type !== "scalar"}
    <p class="hint">{view.items.length.toLocaleString()} items in this reply</p>
  {/if}
  {#if view.type === "json"}
    <div class="jtree"><JsonTree value={view.value} /></div>
    <details class="jrawbox"><summary>Raw JSON</summary><pre class="result jraw">{pretty(view.value)}</pre></details>
  {:else if view.type === "scalar"}
    <div class="result" class:err={view.error}>{view.text || "(empty)"}</div>
  {:else if view.type === "list"}
    {@const window = slice(view.rows)}
    <table>
      <thead><tr><th>{view.title}</th></tr></thead>
      <tbody>
        {#each window.rows as row}
          <tr>
            <td class="mono">{row || "(empty)"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    {@render pager(window.total)}
  {:else if view.type === "pairs"}
    {@const window = slice(view.rows)}
    <table>
      <thead><tr><th>{view.title === "Members" ? "Member" : "Field"}</th><th>{view.title === "Members" ? "Score" : "Value"}</th></tr></thead>
      <tbody>
        {#each window.rows as row}
          <tr>
            <td>{row.field}</td>
            <td class="mono">{row.value || "(empty)"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    {@render pager(window.total)}
  {:else if view.type === "scan"}
    <p class="muted">
      {#if view.cursor === "0"}
        Scan finished.
      {:else}
        More keys remain. Run SCAN again with cursor {view.cursor}.
      {/if}
    </p>
    {@const window = slice(view.rows)}
    <table>
      <thead><tr><th></th><th>Key</th></tr></thead>
      <tbody>
        {#each window.rows as row, index}
          <tr>
            <td class="idx">{window.start + index + 1}</td>
            <td class="mono">{row}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    {@render pager(window.total)}
  {:else if view.type === "entries"}
    {@render entries(view.entries)}
  {:else if view.type === "streams"}
    {#each view.streams as stream}
      <section class="stream">
        <h2>{stream.name}</h2>
        {#if stream.entries.length === 0}
          <p class="muted">No entries</p>
        {:else}
          {@render entries(stream.entries)}
        {/if}
      </section>
    {/each}
  {:else}
    <ol class="tree">
      {#each view.items as item, index}
        <li>
          <span class="idx">{index + 1}</span>
          {#if item?.kind === "array"}
            <ValueView value={item} />
          {:else}
            <span class="mono">{scalar(item) || "(empty)"}</span>
          {/if}
        </li>
      {/each}
    </ol>
  {/if}
</div>
