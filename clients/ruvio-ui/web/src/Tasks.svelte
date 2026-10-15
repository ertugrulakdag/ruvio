<script>
  import { families, taskFamilies, findTasks } from "./catalog.js";
  import Workbench from "./Workbench.svelte";
  let { run, initial = "locking", scopeLabel } = $props();
  let familyId = $state("locking");
  let commandName = $state("LEASE");
  let query = $state("");
  let family = $derived(families.find((item) => item.id === familyId));
  let command = $derived(family.commands.find((item) => item.name === commandName) || family.commands[0]);
  let matches = $derived(query.trim() ? findTasks(query) : []);

  $effect(() => {
    familyId = initial;
    commandName = families.find((item) => item.id === initial)?.commands[0].name || "LEASE";
  });

  function choose(item, action = item.commands[0]) {
    familyId = item.id;
    commandName = action.name;
    query = "";
  }
</script>

<header class="page-head">
  <div><p class="eyebrow">TOOLS FOR THE JOB</p><h1>Tasks</h1><p>Start with what you want to do, not a command to memorize.</p></div>
</header>
<label class="task-search"><span class="sr-only">Find a task or command</span><input type="search" bind:value={query} placeholder="Find a task or command: locking, session, HSET..." /></label>
{#if query.trim()}
  <section class="panel search-results" aria-label="Task search results">
    <p class="eyebrow">{matches.length} MATCHES</p>
    {#each matches as match}
      <button onclick={() => choose(match.family, match.command)}><span><strong>{match.command.blurb}</strong><small>{match.family.label}</small></span><code>{match.command.name}</code></button>
    {/each}
    {#if !matches.length}<p class="empty">No matching task. Try a data type like "list" or a job like "inventory".</p>{/if}
  </section>
{/if}
<div class="task-layout">
  <aside class="task-picker">
    <p class="eyebrow">BY JOB</p>
    {#each taskFamilies.slice(0, 8) as item}
      <button class:on={familyId === item.id} aria-pressed={familyId === item.id} onclick={() => choose(item)}>{item.label}<small>{item.commands[0].name}</small></button>
    {/each}
    <p class="eyebrow">DATA & SERVER</p>
    {#each taskFamilies.slice(8) as item}
      <button class:on={familyId === item.id} aria-pressed={familyId === item.id} onclick={() => choose(item)}>{item.label}</button>
    {/each}
  </aside>
  <div class="task-content">
    <div class="section-heading"><h2>{family.label}</h2><span class="hint">{family.description || "Read, write, and manage values."}</span></div>
    {#if family.commands.length > 1}
      <div class="action-picker" role="group" aria-label={`${family.label} actions`}>
        {#each family.commands as item}
          <button class:on={command.name === item.name} aria-pressed={command.name === item.name} onclick={() => (commandName = item.name)}><span>{item.blurb}</span><code>{item.name}</code></button>
        {/each}
      </div>
    {/if}
    {#if family.id === "changes" || family.id === "server"}
      <div class="notice">These are node-local commands. In All shards, they run on the entry node. Select a shard to target it explicitly.</div>
    {/if}
    <Workbench {command} {run} {scopeLabel} />
  </div>
</div>
