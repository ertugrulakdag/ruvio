<script>
  import { onMount } from "svelte";
  import { families } from "./catalog.js";
  import { scalar } from "./value.js";
  import ValueView from "./ValueView.svelte";
  import Workbench from "./Workbench.svelte";
  let { run } = $props();
  let users = $state([]);
  let selected = $state("");
  let details = $state(null);
  let error = $state("");
  let busy = $state(false);
  const setUser = families.find((item) => item.id === "users").commands.find((item) => item.name === "ACL SETUSER");

  onMount(refresh);

  async function refresh() {
    busy = true;
    try {
      const value = await run(["ACL", "USERS"]);
      if (value.kind !== "array") throw new Error("Unexpected ACL USERS reply.");
      users = value.items.map(scalar);
      error = "";
    } catch (failure) {
      error = failure.message;
    } finally {
      busy = false;
    }
  }

  async function inspect(user) {
    busy = true;
    selected = user;
    details = null;
    try {
      details = await run(["ACL", "GETUSER", user]);
      error = "";
    } catch (failure) {
      error = failure.message;
    } finally {
      busy = false;
    }
  }
</script>

<div class="notice"><strong>Live changes only.</strong> ACL SETUSER from this panel lasts until restart. Put users that must survive a restart in <code>ruvio.toml</code> under <code>[[security.users]]</code>. Existing connections keep their last AUTH rules; reconnect to use changed permissions.</div>
<div class="acl-layout">
  <section class="panel stack">
    <div class="section-heading"><h2>Server users</h2><button disabled={busy} onclick={refresh}>{busy ? "Loading..." : "Refresh"}</button></div>
    <p class="hint">All shards uses the entry node. A pinned shard targets that node.</p>
    {#if error}<div class="error-box" role="alert">{error}</div>{/if}
    <div class="user-list">
      {#each users as user}<button class:on={selected === user} disabled={busy} onclick={() => inspect(user)}><span class="user-avatar" aria-hidden="true">{user.slice(0, 1).toUpperCase()}</span><span>{user}</span><span class="hint">Inspect &rarr;</span></button>{/each}
    </div>
    {#if !users.length && !busy && !error}<p class="empty">No users returned by the server.</p>{/if}
    {#if details}<h2>Access for {selected}</h2><ValueView value={details} command="ACL GETUSER" />{/if}
  </section>
  <div class="stack">
    <Workbench command={setUser} {run} onSuccess={() => { refresh(); details = null; }} />
    <details class="panel"><summary>Rules at a glance</summary><p class="form-help">One rule per line. <code>on</code> enables a user; <code>&gt;password</code> adds a password; <code>~app:*</code> restricts keys; <code>+@read</code> allows reads. Rules are incremental. A new user starts disabled with no access.</p><pre>on{"\n"}&gt;your-secret{"\n"}~app:*{"\n"}+@read{"\n"}+ping{"\n"}+info</pre></details>
  </div>
</div>
