// Fills published package versions from the registries, so no page needs an edit after a release.
// Markup: <span data-pkg="nuget:Ruvio.Client">latest</span>
// Optional: data-found="Published on NuGet, latest {v}." and data-missing="Not published yet."
// The element's own text is the fallback when the lookup fails or the script cannot run.
(function () {
  const endpoints = {
    crates: (name) => `https://img.shields.io/crates/v/${name}.json`,
    nuget: (name) => `https://img.shields.io/nuget/v/${name}.json`,
    docker: (name) => `https://img.shields.io/docker/v/${name}.json?sort=semver`,
    npm: (name) => `https://img.shields.io/npm/v/${name}.json`,
    pypi: (name) => `https://img.shields.io/pypi/v/${name}.json`,
    maven: (name) => `https://img.shields.io/maven-central/v/${name.replace(":", "/")}.json`,
  };

  const lookups = new Map();

  function remember(key, value) {
    try {
      sessionStorage.setItem(`ruvio-version:${key}`, value);
    } catch {
      // Storage can be blocked; the lookup then runs again on the next page.
    }
  }

  function recall(key) {
    try {
      return sessionStorage.getItem(`ruvio-version:${key}`);
    } catch {
      return null;
    }
  }

  // Resolves to a version string, "" when the registry has no such package, or null when unknown.
  function lookup(key) {
    if (lookups.has(key)) {
      return lookups.get(key);
    }

    const [kind, ...rest] = key.split(":");
    const name = rest.join(":");
    const cached = recall(key);
    const make = endpoints[kind];

    const promise = cached !== null
      ? Promise.resolve(cached)
      : !make
        ? Promise.resolve(null)
        : fetch(make(name), { headers: { Accept: "application/json" } })
            .then((response) => (response.ok ? response.json() : Promise.reject(new Error(String(response.status)))))
            .then((data) => {
              const message = String(data.message || data.value || "");

              if (/not found|inaccessible|invalid|no releases|error/i.test(message)) {
                remember(key, "");
                return "";
              }

              const version = message.replace(/^v/i, "").split(/\s+/)[0];

              if (!/^\d+\.\d+\.\d+/.test(version)) {
                return null;
              }

              remember(key, version);
              return version;
            })
            .catch(() => null);

    lookups.set(key, promise);
    return promise;
  }

  async function fill(node) {
    const version = await lookup(node.dataset.pkg);

    if (version === null) {
      return;
    }

    if (version === "") {
      if (node.dataset.missing) {
        node.textContent = node.dataset.missing;
      }

      return;
    }

    node.textContent = node.dataset.found
      ? node.dataset.found.replace("{v}", version)
      : version;
  }

  function run() {
    document.querySelectorAll("[data-pkg]").forEach((node) => {
      fill(node);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
