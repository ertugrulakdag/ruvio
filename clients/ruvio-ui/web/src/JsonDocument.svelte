<script>
  import JsonTree from "./JsonTree.svelte";
  import { parseJson, pretty, kindOf, describe, bytesLabel, resolvePath } from "./json.js";

  // A JSON key: tree, raw text, a path reader and per-path edits. `run(args)` sends
  // one command and returns the reply; `changed()` asks the parent to reload.
  let { name, text, size = 0, truncated = false, run, changed = () => {} } = $props();

  let mode = $state("tree");
  let selected = $state(null);
  let draft = $state("");
  let message = $state("");
  let failure = $state("");
  let busy = $state(false);
  let query = $state("$");
  let queryResult = $state("");
  let amount = $state("1");
  let appendText = $state("");
  let replaceAll = $state(false);
  let replacement = $state("");

  let parsed = $derived(truncated ? { ok: false, error: "Document is too large to load here. Use a path." } : parseJson(text));
  let selectedValue = $derived(selected !== null && parsed.ok ? resolvePath(parsed.value, selected) : undefined);
  let selectedKind = $derived(selectedValue === undefined ? "" : kindOf(selectedValue));

  // After a save the document is reloaded; show the stored value and drop a path that is gone.
  $effect(() => {
    if (selected === null) return;
    if (selectedValue === undefined) selected = null;
    else draft = pretty(selectedValue);
  });

  function pick(path, value) {
    selected = path;
    draft = pretty(value);
    message = "";
    failure = "";
    if (path) query = path;
  }

  async function act(label, args, after) {
    if (busy) return;
    busy = true;
    failure = "";
    message = "";
    try {
      const reply = await run(args);
      if (reply?.kind === "error") throw new Error(reply.text);
      message = label;
      if (after) after(reply);
      changed();
    } catch (error) {
      failure = error.message;
    } finally {
      busy = false;
    }
  }

  function compact(source) {
    const result = parseJson(source);
    if (!result.ok) throw new Error(`Not valid JSON: ${result.error}`);
    return JSON.stringify(result.value);
  }

  function guarded(operation) {
    try {
      operation();
    } catch (error) {
      failure = error.message;
    }
  }

  const saveSelected = () => guarded(() => act(`Saved ${selected}`, ["JSON.SET", name, selected, compact(draft)]));
  const removeSelected = () => act(`Deleted ${selected}`, ["JSON.DEL", name, selected], () => { selected = null; });
  const increment = () => act(`Added ${amount} at ${selected}`, ["JSON.NUMINCRBY", name, selected, amount]);
  const toggle = () => act(`Toggled ${selected}`, ["JSON.TOGGLE", name, selected]);
  const append = () => guarded(() => act(`Appended to ${selected}`, ["JSON.ARRAPPEND", name, selected, compact(appendText)], () => { appendText = ""; }));
  const clear = () => act(`Cleared ${selected}`, ["JSON.CLEAR", name, selected]);
  const saveAll = () => guarded(() => act("Replaced the document", ["JSON.SET", name, "$", compact(replacement)], () => { replaceAll = false; selected = null; }));

  async function ask(event) {
    event.preventDefault();
    queryResult = "";
    failure = "";
    try {
      const reply = await run(["JSON.GET", name, query || "$"]);
      if (reply?.kind === "error") throw new Error(reply.text);
      if (reply?.kind !== "bulk") queryResult = "(nil) no value at that path";
      else {
        const result = parseJson(reply.text);
        queryResult = result.ok ? pretty(result.value) : reply.text;
      }
    } catch (error) {
      failure = error.message;
    }
  }

  function startReplace() {
    replacement = parsed.ok ? pretty(parsed.value) : "";
    replaceAll = !replaceAll;
  }

  async function copy(valueText) {
    try {
      await navigator.clipboard.writeText(valueText);
      message = "Copied";
    } catch {
      message = "Copy is not available in this browser";
    }
  }
</script>

<div class="jdoc">
  <div class="jbar">
    <div class="jmodes" role="tablist" aria-label="Document view">
      <button type="button" role="tab" aria-selected={mode === "tree"} class:on={mode === "tree"} onclick={() => (mode = "tree")}>Tree</button>
      <button type="button" role="tab" aria-selected={mode === "raw"} class:on={mode === "raw"} onclick={() => (mode = "raw")}>Raw</button>
    </div>
    <span class="badge">{parsed.ok ? describe(parsed.value) : "not loaded"}</span>
    {#if size}<span class="badge">{bytesLabel(size)}</span>{/if}
    <span class="jgrow"></span>
    <button type="button" disabled={!parsed.ok} onclick={() => copy(text)}>Copy JSON</button>
    <button type="button" disabled={busy} onclick={startReplace}>{replaceAll ? "Cancel" : "Replace document"}</button>
  </div>

  {#if replaceAll}
    <div class="jedit">
      <label>New document<textarea bind:value={replacement} rows="10" spellcheck="false" aria-label="New document"></textarea></label>
      <div class="actions"><button class="primary" type="button" disabled={busy} onclick={saveAll}>Save document</button></div>
    </div>
  {/if}

  {#if !parsed.ok}
    <p class="muted">{parsed.error}</p>
  {:else if mode === "tree"}
    <div class="jtree" role="tree" aria-label="JSON document">
      <JsonTree value={parsed.value} {selected} onselect={pick} />
    </div>
  {:else}
    <pre class="result jraw">{pretty(parsed.value)}</pre>
  {/if}

  {#if selected !== null && selectedValue !== undefined}
    <section class="jedit" aria-label="Selected value">
      <div class="section-heading"><h3 class="mono">{selected}</h3><span class="badge">{describe(selectedValue)}</span></div>
      <label>Value as JSON<textarea bind:value={draft} rows={selectedKind === "object" || selectedKind === "array" ? 8 : 3} spellcheck="false"></textarea></label>
      <div class="actions">
        <button class="primary" type="button" disabled={busy} onclick={saveSelected}>Save value</button>
        {#if selected !== "$"}<button class="danger" type="button" disabled={busy} onclick={removeSelected}>Delete path</button>{/if}
        {#if selectedKind === "number"}
          <input class="jsmall" bind:value={amount} aria-label="Amount" inputmode="decimal" />
          <button type="button" disabled={busy} onclick={increment}>Add</button>
        {/if}
        {#if selectedKind === "boolean"}<button type="button" disabled={busy} onclick={toggle}>Toggle</button>{/if}
        {#if selectedKind === "object" || selectedKind === "array"}<button type="button" disabled={busy} onclick={clear}>Clear</button>{/if}
        <button type="button" onclick={() => copy(selected)}>Copy path</button>
      </div>
      {#if selectedKind === "array"}
        <div class="find">
          <input bind:value={appendText} aria-label="Value to append" placeholder={'"text", 42 or {"a":1}'} spellcheck="false" />
          <button type="button" disabled={busy || !appendText} onclick={append}>Append</button>
        </div>
      {/if}
    </section>
  {/if}

  <form class="find jquery" onsubmit={ask}>
    <input bind:value={query} aria-label="Path" placeholder="$.field[0]" spellcheck="false" />
    <button type="submit">Read path</button>
  </form>
  {#if queryResult}<pre class="result jraw" aria-label="Path result">{queryResult}</pre>{/if}
  {#if message}<p class="muted" role="status">{message}</p>{/if}
  {#if failure}<p class="error-box" role="alert">{failure}</p>{/if}
</div>
