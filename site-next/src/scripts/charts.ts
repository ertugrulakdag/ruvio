// Client-side behaviour shared by all charts: hover tooltips and the platform switch.
// Charts are static SVG rendered at build time; this script only adds interaction.

function initTooltips() {
  document.querySelectorAll<HTMLElement>("[data-chart]").forEach((chart) => {
    if (chart.dataset.tipReady) return;
    chart.dataset.tipReady = "1";
    const tip = document.createElement("div");
    tip.className = "chart-tip";
    tip.hidden = true;
    tip.setAttribute("role", "status");
    chart.appendChild(tip);
    let active: Element | null = null;
    const show = (el: Element, x: number, y: number) => {
      const b = chart.getBoundingClientRect();
      const title = el.getAttribute("data-tip") ?? "";
      const sub = el.getAttribute("data-tip-sub") ?? "";
      const sub2 = el.getAttribute("data-tip-sub2") ?? "";
      tip.replaceChildren();
      const t = document.createElement("b");
      t.textContent = title;
      tip.append(t);
      if (sub) tip.append(document.createTextNode(sub));
      if (sub2) {
        tip.append(document.createElement("br"));
        const s = document.createElement("span");
        s.textContent = sub2;
        tip.append(s);
      }
      tip.hidden = false;
      const left = Math.min(x - b.left + 14, b.width - tip.offsetWidth - 4);
      tip.style.left = `${Math.max(0, left)}px`;
      tip.style.top = `${y - b.top + 14}px`;
      if (active !== el) {
        active?.classList.remove("is-on");
        el.classList.add("is-on");
        chart.classList.add("has-focus");
        active = el;
      }
    };
    const hide = () => {
      tip.hidden = true;
      active?.classList.remove("is-on");
      chart.classList.remove("has-focus");
      active = null;
    };
    chart.addEventListener("pointermove", (e) => {
      const el = (e.target as Element).closest("[data-tip]");
      if (el && chart.contains(el)) show(el, e.clientX, e.clientY);
      else hide();
    });
    chart.addEventListener("pointerleave", hide);
    chart.addEventListener("focusin", (e) => {
      const el = (e.target as Element).closest("[data-tip]");
      if (!el) return;
      const r = el.getBoundingClientRect();
      show(el, r.left + r.width / 2, r.top + r.height / 2);
    });
    chart.addEventListener("focusout", hide);
  });
}

function initPlatformSwitch() {
  document.querySelectorAll<HTMLElement>("[data-platform-scope]").forEach((scope) => {
    if (scope.dataset.platformReady) return;
    scope.dataset.platformReady = "1";
    const set = (p: string) => {
      scope.querySelectorAll<HTMLElement>("[data-platform]").forEach((el) => (el.hidden = el.dataset.platform !== p));
      scope.querySelectorAll<HTMLButtonElement>("[data-set-platform]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.setPlatform === p)));
    };
    scope.querySelectorAll<HTMLButtonElement>("[data-set-platform]").forEach((b) => b.addEventListener("click", () => set(b.dataset.setPlatform!)));
  });
}

initTooltips();
initPlatformSwitch();
