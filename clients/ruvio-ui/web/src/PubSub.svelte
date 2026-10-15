<script>
  import { onMount } from "svelte";
  import { api } from "./api.js";
  let { target, note, endpoint, busy = $bindable(false), database = $bindable("0") } = $props();
  let channel = $state("");
  let pattern = $state("");
  let outgoing = $state("");
  let subscriptions = $state([]);
  let messages = $state([]);
  let error = $state("");
  let notice = $state("");
  let polling = false;
  let alive = true;

  onMount(() => {
    const timer = setInterval(poll, 800);
    return () => { alive = false; clearInterval(timer); };
  });

  async function poll() {
    if (polling || !subscriptions.length) return;
    polling = true;
    try {
      const result = await api("/api/pubsub/poll");
      if (!alive) return;
      const received = result.messages.map((message) => ({ ...message, time: new Date().toLocaleTimeString() }));
      messages = [...received.reverse(), ...messages].slice(0, 200);
      for (const message of received) {
        if (message.kind === "error") {
          error = message.text;
          notice = "";
          subscriptions = [];
          note("SUBSCRIBE", message.text, false);
        }
      }
    } catch (failure) {
      if (alive) error = failure.message;
    } finally {
      polling = false;
    }
  }

  async function listen(name, isPattern, subscribe, isSharded = false) {
    if (busy) return;
    busy = true;
    notice = "";
    try {
      await api(`/api/pubsub/${subscribe ? "subscribe" : "unsubscribe"}`, {
        channels: isPattern || isSharded ? [] : [name],
        patterns: isPattern ? [name] : [],
        sharded_channels: isSharded ? [name] : [],
        shard: target,
      });
      subscriptions = subscriptions.filter((item) => !(item.name === name && item.isPattern === isPattern && item.isSharded === isSharded));
      if (subscribe) subscriptions = [...subscriptions, { name, isPattern, isSharded }];
      notice = subscribe ? `Listening on ${endpoint}.` : `Stopped listening to ${name}.`;
      error = "";
      const command = isSharded ? "SSUBSCRIBE" : isPattern ? "PSUBSCRIBE" : "SUBSCRIBE";
      note(subscribe ? command : isSharded ? "SUNSUBSCRIBE" : isPattern ? "PUNSUBSCRIBE" : "UNSUBSCRIBE", notice);
    } catch (failure) {
      error = failure.message;
      note("PUB/SUB", error, false);
    } finally {
      busy = false;
    }
  }

  function listenForKeyEvent(eventName) {
    const db = Number(database);
    if (!Number.isInteger(db) || db < 0 || db > 255) {
      error = "Database must be an integer from 0 to 255.";
      return;
    }
    const name = `__keyevent@${db}__:${eventName}`;
    const active = subscriptions.some((item) => item.name === name && item.isSharded);
    listen(name, false, !active, true);
  }

  async function publish(event) {
    event.preventDefault();
    busy = true;
    notice = "";
    try {
      const result = await api("/api/pubsub/publish", { channel, message: outgoing, shard: target });
      notice = `Published to ${result.receivers} receiver(s) on ${endpoint}.`;
      error = "";
      note("PUBLISH", notice);
    } catch (failure) {
      error = failure.message;
      note("PUBLISH", error, false);
    } finally {
      busy = false;
    }
  }
</script>

<header class="page-head"><div><p class="eyebrow">REAL-TIME MESSAGING</p><h1>Pub/Sub</h1><p>Listen to channels and patterns, or send a message.</p></div><span class="badge">{subscriptions.length} subscriptions</span></header>
<div class="notice">Pub/Sub uses {endpoint}{target ? " (pinned shard)" : " (entry node in All shards)"}. Changing shard scope stops subscriptions and clears this view.</div>
{#if error}<div class="error-box" role="alert">{error}</div>{/if}
{#if notice}<p class="success-note" role="status">{notice}</p>{/if}
<div class="pubsub">
  <div class="stack">
    <section class="panel stack">
      <h2>Listen</h2>
      <form class="stack" onsubmit={(event) => { event.preventDefault(); listen(channel, false, true); }}>
        <label>Channel<input bind:value={channel} required placeholder="orders.created" /></label>
        <button disabled={busy} class="primary">Subscribe to channel</button>
      </form>
      <form class="stack" onsubmit={(event) => { event.preventDefault(); listen(pattern, true, true); }}>
        <label>Pattern<input bind:value={pattern} required placeholder="orders.*" /></label>
        <button disabled={busy}>Subscribe to pattern</button>
      </form>
      <div class="stack key-events">
        <h3>Key events</h3>
        <label>Database<input bind:value={database} type="number" min="0" max="255" step="1" /></label>
        <p class="hint">Requires <code>notify_keyspace_events = "Exe"</code> in ruvio.toml. Events are limited to this node; All shards listens on the entry node.</p>
        <div class="event-actions">
          <button disabled={busy} onclick={() => listenForKeyEvent("expired")}>
            {subscriptions.some((item) => item.isSharded && item.name === `__keyevent@${Number(database)}__:expired`) ? "Stop expired events" : "Listen for expired"}
          </button>
          <button disabled={busy} onclick={() => listenForKeyEvent("evicted")}>
            {subscriptions.some((item) => item.isSharded && item.name === `__keyevent@${Number(database)}__:evicted`) ? "Stop evicted events" : "Listen for evicted"}
          </button>
        </div>
      </div>
      {#each subscriptions as subscription}
        <div class="subscription"><div><span class="hint">{subscription.isSharded ? "Sharded channel" : subscription.isPattern ? "Pattern" : "Channel"}</span><p class="mono">{subscription.name}</p></div><button class="quiet" disabled={busy} onclick={() => listen(subscription.name, subscription.isPattern, false, subscription.isSharded)}>Unsubscribe</button></div>
      {/each}
    </section>
    <form class="panel stack" onsubmit={publish}>
      <h2>Publish</h2>
      <label>Destination channel<input bind:value={channel} required placeholder="orders.created" /></label>
      <label>Message<textarea bind:value={outgoing} placeholder="Message payload" spellcheck="false"></textarea></label>
      <button class="primary" disabled={busy}>Publish message</button>
    </form>
  </div>
  <section class="panel messages">
    <div class="section-heading"><div><h2>Messages</h2><p class="hint">Latest first / last 200 messages</p></div><button class="quiet" onclick={() => (messages = [])}>Clear</button></div>
    {#if !messages.length}<div class="empty-state"><span class="empty-symbol" aria-hidden="true">&harr;</span><h2>{subscriptions.length ? "Listening for messages" : "Start a conversation"}</h2><p>Subscribe to a channel or pattern. Incoming messages will appear here.</p></div>{/if}
    <div class="message-feed">
      {#each messages as message}
        <article class="message"><div class="section-heading"><code>{message.channel || message.kind}</code><time class="hint">{message.time}</time></div><p class="hint">{message.kind}{message.pattern ? ` / ${message.pattern}` : ""}</p><pre class:err={message.kind === "error"}>{message.text ?? "(binary payload)"}</pre></article>
      {/each}
    </div>
  </section>
</div>
