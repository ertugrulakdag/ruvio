<script>
  import JsonTree from "./JsonTree.svelte";
  import { kindOf, childPath, entries, preview, describe } from "./json.js";

  let { value, name = "", path = "$", depth = 0, selected = null, onselect = () => {}, openDepth = 1 } = $props();

  const window = 100;
  let open = $state(depth < openDepth);
  let shown = $state(window);

  let kind = $derived(kindOf(value));
  let branch = $derived(kind === "object" || kind === "array");
  let rows = $derived(branch ? entries(value) : []);
  let chosen = $derived(path !== null && selected === path);
</script>

<div class="jnode" style:--depth={depth}>
  <div class="jline" class:on={chosen}>
    {#if branch}
      <button type="button" class="jtoggle" aria-expanded={open} aria-label={open ? "Collapse" : "Expand"} onclick={() => (open = !open)}>{open ? "▾" : "▸"}</button>
    {:else}
      <span class="jtoggle" aria-hidden="true"></span>
    {/if}
    <button type="button" class="jselect" disabled={path === null} title={path ?? "This key has no path in Ruvio's path language"} onclick={() => { if (branch && chosen) open = !open; else if (branch) open = true; onselect(path, value); }}>
      {#if name !== ""}<span class="jkey">{typeof name === "number" ? name : JSON.stringify(name)}</span><span class="jcolon">:</span>{/if}
      {#if branch}
        <span class="jmeta">{describe(value)}</span>
      {:else}
        <span class={`jval j-${kind}`}>{preview(value, 200)}</span>
      {/if}
    </button>
  </div>
  {#if branch && open}
    <div class="jchildren">
      {#each rows.slice(0, shown) as [key, item]}
        <JsonTree value={item} name={key} path={childPath(path, key)} depth={depth + 1} {selected} {onselect} {openDepth} />
      {/each}
      {#if rows.length > shown}
        <button type="button" class="jmore" onclick={() => (shown += window)}>Show {Math.min(window, rows.length - shown)} more of {rows.length - shown}</button>
      {/if}
      {#if rows.length === 0}<span class="jmeta jempty">empty</span>{/if}
    </div>
  {/if}
</div>
