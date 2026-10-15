<script>
  import ValueView from "./ValueView.svelte";

  let { command, run, scopeLabel = "", onSuccess = () => {} } = $props();
  let fields = $state({});
  let result = $state(null);
  let busy = $state(false);

  $effect(() => {
    const next = {};
    for (const field of command.fields) next[field.name] = field.choices?.[0]?.value ?? "";
    fields = next;
  });

  async function submit(event) {
    event.preventDefault();
    busy = true;
    const selected = command;
    const mode = fields.what || "both";
    try {
      const args = selected.build(fields).map((value) => String(value ?? ""));
      const output = await run(args);
      result = { output, command: selected.name, mode, failed: false };
      onSuccess(selected.name);
    } catch (error) {
      result = { output: error.message, command: selected.name, failed: true };
    } finally {
      busy = false;
    }
  }
</script>

<div class="workbench">
  <form class="panel form" onsubmit={submit}>
    <div class="section-heading"><p class="eyebrow">ACTION</p><code class="badge">{command.name}</code></div>
    <div><h2>{command.blurb}</h2>{#if command.help}<p class="form-help">{command.help}</p>{/if}</div>
    <fieldset disabled={busy}>
      {#each command.fields as field}
        <label>
          {field.label}{#if field.optional}<span class="optional">optional</span>{/if}
          {#if field.choices}
            <select bind:value={fields[field.name]}>
              {#each field.choices as choice}<option value={choice.value}>{choice.label}</option>{/each}
            </select>
          {:else if field.lines || field.area}
            <textarea bind:value={fields[field.name]} required={!field.optional} placeholder={field.placeholder || "One item per line"} spellcheck="false"></textarea>
          {:else}
            <input bind:value={fields[field.name]} type={field.type || "text"} min={field.min} step={field.step} required={!field.optional && (field.required || (!field.placeholder && field.name !== "value"))} inputmode={field.inputmode || "text"} placeholder={field.placeholder || ""} autocomplete="off" spellcheck="false" />
          {/if}
          {#if field.hint}<span class="hint">{field.hint}</span>{/if}
        </label>
      {/each}
    </fieldset>
    <div class="form-footer"><button class="primary" disabled={busy}>{busy ? "Running..." : command.blurb}</button><span class="hint">{scopeLabel}</span></div>
  </form>
  {#if result}
    <section class="panel response" aria-label="Command result" aria-live="polite">
      <div class="section-heading"><h2>{result.failed ? "Could not complete action" : "Result"}</h2><code class="badge">{result.command}</code></div>
      {#if result.failed}
        <div class="error-box" role="alert">{result.output}</div>
        <p class="hint">The server reply is kept here. Adjust the inputs or choose another action and try again.</p>
      {:else}
        <ValueView value={result.output} command={result.command} mode={result.mode} />
      {/if}
    </section>
  {:else}
    <div class="result-placeholder"><span class="status-dot idle"></span> Results appear here after you run an action.</div>
  {/if}
</div>
