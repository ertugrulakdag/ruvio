const sources = [
  {
    id: "count-ruvio-client",
    crate: "ruvio-client",
    badge: "https://img.shields.io/crates/d/ruvio-client.json",
  },
  {
    id: "count-nuget-client",
    nuget: "Ruvio.Client",
    badge: "https://img.shields.io/nuget/dt/Ruvio.Client.json",
  },
  {
    id: "count-nuget-aspnet",
    nuget: "Ruvio.Client.AspNetCore",
    badge: "https://img.shields.io/nuget/dt/Ruvio.Client.AspNetCore.json",
  },
  {
    id: "count-docker",
    docker: "salihcantekin/ruvio",
    badge: "https://img.shields.io/docker/pulls/salihcantekin/ruvio.json",
  },
];

function formatCount(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString("en-US");
  }

  return String(value);
}

function setCount(id, text) {
  const node = document.getElementById(id);

  if (node) {
    node.textContent = text;
  }
}

async function readJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });

  if (!response.ok) {
    throw new Error(String(response.status));
  }

  return response.json();
}

async function crateDownloads(name) {
  const data = await readJson(`https://crates.io/api/v1/crates/${name}`);

  return data.crate.downloads;
}

async function nugetDownloads(id) {
  const query = new URL("https://azuresearch-usnc.nuget.org/query");

  query.searchParams.set("q", `packageid:${id}`);
  query.searchParams.set("prerelease", "true");

  const data = await readJson(query);
  const pack = data.data && data.data[0];

  if (!pack) {
    throw new Error("missing");
  }

  return pack.totalDownloads;
}

async function dockerPulls(name) {
  const data = await readJson(`https://hub.docker.com/v2/repositories/${name}/`);

  return data.pull_count;
}

async function badgeMessage(url) {
  const data = await readJson(url);
  const message = data.message || data.value;

  if (!message || /not found|invalid|error/i.test(message)) {
    throw new Error(message || "badge");
  }

  return message;
}

async function loadOne(source) {
  try {
    if (source.crate) {
      setCount(source.id, formatCount(await crateDownloads(source.crate)));
      return;
    }

    if (source.nuget) {
      setCount(source.id, formatCount(await nugetDownloads(source.nuget)));
      return;
    }

    if (source.docker) {
      setCount(source.id, formatCount(await dockerPulls(source.docker)));
      return;
    }
  } catch {
    // The registry APIs often refuse a browser origin. The badge JSON is public.
  }

  try {
    setCount(source.id, await badgeMessage(source.badge));
  } catch {
    if (source.docker) {
      setCount(source.id, "private");
    }
  }
}

async function loadCounts() {
  await Promise.all(sources.map(loadOne));

  const stamp = document.getElementById("counts-updated");

  if (stamp) {
    stamp.textContent = `Counts refresh when this page loads. Last attempt ${new Date().toISOString()}.`;
  }
}

loadCounts();
