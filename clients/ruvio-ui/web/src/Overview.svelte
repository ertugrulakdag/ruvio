<script>
  import { execute, nodeId, parseInfo, metric, totalMetric, number, bytes } from "./operator.js";
  import { taskFamilies } from "./catalog.js";
  import Icon from "./Icon.svelte";
  import { preference, savePreference } from "./preferences.js";
  const taskIcons = { locking: "lock", conditional: "layers", limiting: "clock", idempotency: "key", inventory: "cart", sessions: "users", jobs: "tasks", changes: "list", json: "doc", strings: "doc", keys: "key", lists: "list" };
  let { shards, shard, openTask, onData, lines } = $props();
  let readings = $state([]);
  let refreshing = $state(false);
  let shardsOpen = $state(preference("overview.shardsOpen", false) === true);
  let paused = $state(preference("overview.paused", false) === true);
  const savedInterval = Number(preference("overview.interval", 5));
  let interval = $state([2, 5, 10, 30, 60].includes(savedInterval) ? savedInterval : 5);
  let updated = $state("");
  let refreshNow = $state(() => {});
  let visible = $derived(shard ? readings.filter((item) => nodeId(item.node) === nodeId(shard)) : readings);
  let up = $derived(readings.filter((item) => item.reachable).length);
  let commands = $derived(totalMetric(visible, "total_commands_processed"));
  let commandGroups = [
    { label: "Reads", field: "get_commands" },
    { label: "Writes", field: "set_commands" },
    { label: "Increments", field: "increment_commands" },
    { label: "Other", field: "other_commands" },
  ];

  $effect(() => {
    savePreference("overview.paused", paused);
    savePreference("overview.interval", interval);
  });

  $effect(() => {
    const nodes = shards;
    const isPaused = paused;
    const seconds = interval;
    let alive = true;
    let pending = false;
    async function refresh() {
      if (pending || !nodes.length) return;
      pending = true;
      refreshing = true;
      const results = await Promise.all(nodes.map(async (node) => {
        const start = performance.now();
        try {
          const info = parseInfo(await execute(["INFO"], node));
          return { node, info, reachable: true, ms: performance.now() - start, error: "" };
        } catch (error) {
          let reachable = false;
          try {
            await execute(["PING"], node);
            reachable = true;
          } catch (pingError) {
            return { node, info: null, reachable, error: `INFO: ${error.message} / PING: ${pingError.message}` };
          }
          return { node, info: null, reachable, error: `INFO: ${error.message}` };
        }
      }));
      if (alive) {
        readings = results;
        updated = new Date().toLocaleTimeString();
        refreshing = false;
      }
      pending = false;
    }
    refreshNow = refresh;
    refresh();
    const timer = isPaused ? null : setInterval(refresh, seconds * 1000);
    return () => { alive = false; clearInterval(timer); };
  });
</script>

<header class="page-head">
  <div><p class="eyebrow">LIVE PICTURE</p><h1>Overview</h1><p>Your Ruvio, at a glance.</p></div>
  <div class="actions"><label class="refresh-interval">Refresh interval<select bind:value={interval}>{#each [2, 5, 10, 30, 60] as seconds}<option value={seconds}>{seconds}s</option>{/each}</select></label><span class="live-label"><span class="status-dot" class:idle={paused}></span>{paused ? "Paused" : `Live / ${interval}s`}</span><button class="quiet" onclick={() => (paused = !paused)}>{paused ? "Resume" : "Pause"}</button><button disabled={refreshing || !shards.length} onclick={refreshNow}>{refreshing ? "Refreshing..." : "Refresh"}</button></div>
</header>
<div class="metrics">
  <article class="metric-card"><p>Shards responding</p><strong>{readings.length ? `${up} / ${readings.length}` : "--"}</strong><span class="hint">{readings.length ? up === readings.length ? "All discovered nodes reachable" : "Some nodes need attention" : "Waiting for shard discovery"}</span></article>
  <article class="metric-card"><p>Keys</p><strong class:unavailable={totalMetric(visible, "keys") === null}>{number(totalMetric(visible, "keys"))}</strong><button class="text-button" onclick={onData}>Explore data &rarr;</button></article>
  <article class="metric-card"><p>Charged memory</p><strong class:unavailable={totalMetric(visible, "charged_memory_bytes") === null}>{bytes(totalMetric(visible, "charged_memory_bytes"))}</strong><span class="hint">Store memory, not process RSS</span></article>
  <article class="metric-card"><p>Commands processed</p><strong class:unavailable={commands === null}>{number(commands)}</strong><span class="hint">Server lifetime / when reported</span></article>
</div>
<p class="metrics-caption">{shard ? `Metrics for ${nodeId(shard)}` : "Metrics summed across discovered shards"} &middot; {updated ? `Last checked ${updated}` : "First reading in progress"} &middot; Missing metrics are never counted as zero.</p>

<section class="panel">
  <div class="section-heading">
    <div class="shard-head">
      <h2><button class="shard-toggle" aria-expanded={shardsOpen} aria-controls="shard-grid" onclick={() => { shardsOpen = !shardsOpen; savePreference("overview.shardsOpen", shardsOpen); }}>
        <Icon name={shardsOpen ? "chevronDown" : "chevronRight"} />Shard health
      </button></h2>
      <p class="hint">Availability, storage, and persistence by owner</p>
    </div>
    <span class="badge">{#if readings.length}{readings.filter((item) => item.reachable).length} of {readings.length} up{:else}{shards.length} discovered{/if}</span>
  </div>
  <div class="shard-grid" id="shard-grid" hidden={!shardsOpen}>
    {#each readings as reading}
      {@const info = reading.info}
      {@const unhealthy = info && (info.ruvio_persistence_state === "error" || (info.ruvio_readiness && info.ruvio_readiness !== "ready") || (info.ruvio_health && info.ruvio_health !== "ok"))}
      <article class="shard-card">
        <div class="section-heading"><strong>Shard {reading.node.id}</strong><span class="health" class:err={!reading.reachable || unhealthy} class:warning={reading.reachable && !info}><span class="status-dot" class:down={!reading.reachable || unhealthy}></span>{!reading.reachable ? "Unavailable" : unhealthy ? "Needs attention" : !info ? "Reachable / no metrics" : "Up"}</span></div>
        <p class="mono muted">{nodeId(reading.node)}</p>
        <dl><div><dt>Keys</dt><dd>{number(metric(info, "keys"))}</dd></div><div><dt>Memory</dt><dd>{bytes(metric(info, "charged_memory_bytes"))}</dd></div><div><dt>Persistence</dt><dd>{info?.ruvio_persistence_state || "Not reported"}</dd></div><div><dt>Durability</dt><dd>{info?.ruvio_durability || "Not reported"}</dd></div></dl>
        <dl>
          {#each [{ label: "Connected clients", field: "connected_clients" }, { label: "Expiring keys", field: "expires" }, { label: "Expired keys", field: "expired_keys" }, { label: "WAL sequence", field: "wal_last_sequence" }] as item}
            {#if metric(info, item.field) !== null}<div><dt>{item.label}</dt><dd>{number(metric(info, item.field))}</dd></div>{/if}
          {/each}
          {#if metric(info, "wal_unsynced_bytes") !== null}<div><dt>Unsynced WAL</dt><dd>{bytes(metric(info, "wal_unsynced_bytes"))}</dd></div>{/if}
          {#if info?.replication_role}<div><dt>Replication</dt><dd>{info.replication_role}</dd></div>{/if}
        </dl>
        <dl>
          {#each [{ label: "Key bytes", field: "key_memory_bytes" }, { label: "Value bytes", field: "value_memory_bytes" }, { label: "Collections", field: "collection_memory_bytes" }, { label: "Index capacity", field: "index_memory_bytes" }, { label: "Reusable buffers", field: "pooled_value_bytes" }] as item}
            {#if metric(info, item.field) !== null}<div><dt>{item.label}</dt><dd>{bytes(metric(info, item.field))}</dd></div>{/if}
          {/each}
        </dl>
        <p class="hint">Slots {reading.node.start}&ndash;{reading.node.end}{reading.ms == null ? "" : ` / INFO ${Math.round(reading.ms)} ms`}</p>
        {#if reading.error}<div class="error-box" role="alert">{reading.error}</div>{/if}
      </article>
    {/each}
    {#if !readings.length}<p class="empty">{refreshing ? "Reading each shard..." : "No readings yet. Discover shards or refresh to retry."}</p>{/if}
  </div>
</section>

<section class="quick-section">
  <div class="section-heading"><div><h2>What do you need to do?</h2><p class="hint">Purpose-built forms. Real server commands.</p></div><button class="text-button" onclick={() => openTask("strings")}>All tasks &rarr;</button></div>
  <div class="quick-grid">
    {#each taskFamilies.slice(0, 8) as item, index}
      <button class="quick-card" onclick={() => openTask(item.id)}><span class="quick-icon"><Icon name={taskIcons[item.id] ?? "bolt"} /></span><strong>{item.label}</strong><span>{item.description}</span><code>{item.commands[0].name}{item.id === "locking" ? " / RELEASE" : ""}</code><span class="quick-arrow" aria-hidden="true">&nearr;</span></button>
    {/each}
  </div>
</section>
<div class="overview-bottom">
  <section class="panel"><div class="section-heading"><h2>Command activity</h2><span class="hint">Server counters</span></div>
    {#each commandGroups as group}
      {@const value = totalMetric(visible, group.field)}
      <div class="counter-row"><span>{group.label}</span><strong>{number(value)}</strong></div>
    {/each}
    <p class="hint">Counts include other clients and this panel's reads. GET, SET and INCR have separate counters; other commands are grouped together.</p>
  </section>
  <section class="panel"><div class="section-heading"><h2>Recent panel activity</h2><span class="hint">This session</span></div>
    {#each lines.slice(0, 5) as line}
      <div class="activity-row"><span class="status-dot" class:down={!line.ok}></span><code>{line.command}</code><span class="hint">{line.time}</span></div>
    {/each}
    {#if !lines.length}<p class="empty">Your actions will appear here.</p>{/if}
    <p class="hint">Open Session activity below for replies and errors.</p>
  </section>
</div>
