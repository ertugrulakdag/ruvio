(function () {
  const here = (document.body.getAttribute("data-page") || "").replace(/^\//, "");
  document.querySelectorAll(".top nav a").forEach((link) => {
    const href = link.getAttribute("href") || "";
    if (href === here || (here.startsWith("testing-") && href === "testing.html")) {
      link.setAttribute("aria-current", href === here ? "page" : "location");
    }
  });

  const navigation = document.querySelector(".top nav");
  if (!navigation) return;

  const groups = [...navigation.querySelectorAll("details")];
  const canHover = window.matchMedia("(hover: hover) and (pointer: fine)");

  function closeGroups(except) {
    groups.forEach((group) => {
      if (group !== except) group.open = false;
    });
  }

  groups.forEach((group) => {
    group.addEventListener("mouseenter", () => {
      if (!canHover.matches) return;
      closeGroups(group);
      group.open = true;
    });

    group.addEventListener("mouseleave", () => {
      if (canHover.matches && !group.querySelector(":focus-visible")) {
        group.open = false;
      }
    });

    group.addEventListener("focusout", () => {
      window.setTimeout(() => {
        if (!group.contains(document.activeElement) && !group.matches(":hover")) {
          group.open = false;
        }
      });
    });

    group.addEventListener("toggle", () => {
      if (group.open) closeGroups(group);
    });
  });

  document.addEventListener("pointerdown", (event) => {
    if (!groups.some((group) => group.contains(event.target))) closeGroups();
  });

})();

// A link to a table row: once fonts and layout settle, put that row about 40% down the
// screen. The browser's own jump happens before the page has its final height.
(function () {
  function alignTarget() {
    const id = decodeURIComponent(location.hash.slice(1));
    const row = id && document.getElementById(id);
    if (row && row.matches("tr[id]")) {
      row.scrollIntoView({ block: "start", behavior: "instant" });
    }
  }

  if (!location.hash) return;
  const settle = () => (document.fonts ? document.fonts.ready : Promise.resolve()).then(alignTarget);
  if (document.readyState === "complete") settle();
  else window.addEventListener("load", settle);
  window.addEventListener("hashchange", alignTarget);
})();

// Section icons: each article h2 gets a small line icon chosen by its words. Decorative only.
(function () {
  const icons = [
    [/config|setting|flag|environment|option/, '<path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M18 18h2"/><circle cx="16" cy="6" r="2"/><circle cx="10" cy="12" r="2"/><circle cx="16" cy="18" r="2"/>'],
    [/durab|persist|wal|snapshot|disk|recover|backup|checkpoint|storage/, '<ellipse cx="12" cy="6" rx="7.5" ry="2.8"/><path d="M4.5 6v12c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8V6"/><path d="M4.5 12c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8"/>'],
    [/replic|cluster|shard|node|failover/, '<circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="12" cy="18" r="2.5"/><path d="M8 7.5l3 8M16 7.5l-3 8M8.5 6h7"/>'],
    [/test|perf|bench|measure|memory|latency|result/, '<path d="M4 16a8 8 0 1 1 16 0"/><path d="M12 16l4-4"/><circle cx="12" cy="16" r="1"/>'],
    [/client|package|sdk|install|example|docker|import/, '<path d="M12 3l8 4.5v9L12 21l-8-4.5v-9z"/><path d="M4 7.5l8 4.5 8-4.5M12 12v9"/>'],
    [/security|auth|acl|tls|password|permission|threat|sign|user|access/, '<path d="M12 3l8 3v6c0 4.5-3.4 8-8 9-4.6-1-8-4.5-8-9V6z"/><path d="M9 12l2 2 4-4"/>'],
    [/operat|metric|monitor|observ|log|health|screen|overview|dashboard|do\?/, '<path d="M3 12h4l3-7 4 14 3-7h4"/>'],
    [/transaction|lua|script|multi|command|protocol|keyspace|key/, '<path d="M8 8l-4 4 4 4M16 8l4 4-4 4M14 5l-4 14"/>'],
  ];
  const fallback = '<path d="M5 12h14M13 6l6 6-6 6"/>';
  document.querySelectorAll("article h2").forEach((h) => {
    const words = h.textContent.toLowerCase();
    const hit = icons.find(([re]) => re.test(words));
    const body = hit ? hit[1] : fallback;
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "sec-icon");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("aria-hidden", "true");
    svg.innerHTML = body;
    h.prepend(svg);
  });

  // In-page menu: highlight the section on screen. Near the bottom of a page the last
  // section can never reach the top line, so the last link is used once the page is at its end.
  const side = document.querySelector("nav.side");
  if (!side) return;
  // Narrow screens: the same links sit in a collapsed "On this page" drawer above the text
  // (CSS shows the drawer and hides the side menu below the layout breakpoint).
  const drawer = document.createElement("details");
  drawer.className = "side-drawer";
  drawer.innerHTML = "<summary>On this page</summary>";
  const drawerNav = document.createElement("nav");
  drawerNav.setAttribute("aria-label", side.getAttribute("aria-label") || "On this page");
  drawerNav.innerHTML = side.innerHTML;
  drawer.append(drawerNav);
  side.before(drawer);
  const links = [...side.querySelectorAll('a[href^="#"]')];
  const menuLinks = [...document.querySelectorAll('nav.side a[href^="#"], .side-drawer a[href^="#"]')];
  const pairs = links
    .map((a) => ({ a, target: document.getElementById(a.getAttribute("href").slice(1)) }))
    .filter((p) => p.target);
  if (!pairs.length) return;
  // Both menus are marked by target, so the drawer and the side menu always agree.
  const mark = (link) => {
    const href = link.getAttribute("href");
    menuLinks.forEach((a) => a.classList.toggle("is-active", a.getAttribute("href") === href));
  };
  // A clicked entry stays selected until the reader scrolls by hand (wheel, touch, keys),
  // so the page's own scroll to that section does not move the highlight.
  let locked = null;
  const unlock = () => {
    if (!locked) return;
    locked = null;
    update();
  };
  const update = () => {
    if (locked) return mark(locked);
    const atEnd = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4;
    if (atEnd) return mark(pairs[pairs.length - 1].a);
    const line = window.innerHeight * 0.3;
    let current = pairs[0];
    pairs.forEach((p) => {
      if (p.target.getBoundingClientRect().top <= line) current = p;
    });
    mark(current.a);
  };
  window.addEventListener("scroll", update, { passive: true });
  window.addEventListener("resize", update);
  document.addEventListener("click", (event) => {
    const link = event.target.closest('nav.side a[href^="#"], .side-drawer a[href^="#"]');
    if (!link) return;
    if (link.closest(".side-drawer")) drawer.open = false;
    locked = link;
    mark(link);
  });
  ["wheel", "touchstart", "keydown"].forEach((name) => window.addEventListener(name, unlock, { passive: true }));
  update();
})();

// Code blocks: colour each one by its language and label it. Shell and TOML are recognised
// by their first words; other blocks take the page's language (client-rust -> Rust, and so on),
// and pages without one let highlight.js pick from a short list.
(function () {
  const page = (location.pathname.split("/").pop() || "").replace(/\.html$/, "");
  const pageLang = {
    "client-csharp": "csharp", "client-rust": "rust", "client-go": "go",
    "client-java": "java", "client-python": "python", "client-node": "javascript",
  }[page] || null;
  const names = {
    bash: "Shell", csharp: "C#", rust: "Rust", go: "Go", java: "Java", python: "Python",
    javascript: "JavaScript", typescript: "TypeScript", ini: "TOML", json: "JSON",
    yaml: "YAML", sql: "SQL", plaintext: "Text",
  };
  const guessFrom = ["bash", "python", "javascript", "rust", "go", "csharp", "java", "ini", "json", "sql"];
  document.querySelectorAll("pre").forEach((pre) => {
    const code = pre.textContent;
    let lang = pageLang;
    if (/^\s*(\$ |#|dotnet |cargo |docker |go |npm |npx |pip |mvn |git |curl |export |\.\/|ruvio |redis-cli )/m.test(code)) lang = "bash";
    else if (/^\[[A-Za-z0-9_.\[\]-]+\]\s*$/m.test(code) && /^\s*[A-Za-z_][\w.-]*\s*=/m.test(code)) lang = "ini";
    if (!window.hljs) {
      pre.dataset.lang = names[lang] || "";
      return;
    }
    let result;
    if (lang && hljs.getLanguage(lang)) {
      result = hljs.highlight(code, { language: lang, ignoreIllegals: true });
    } else {
      result = hljs.highlightAuto(code, guessFrom);
      lang = result.language;
    }
    if (!result || !lang) return;
    pre.innerHTML = `<code class="hljs language-${lang}">${result.value}</code>`;
    pre.dataset.lang = names[lang] || lang;
  });
})();
