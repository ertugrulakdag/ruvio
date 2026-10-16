// Turns a plain code string into escaped HTML with two quiet accents: comments dimmed, strings tinted.
// Comments are whole lines starting with "#" or "//", or a trailing comment after two or more spaces.
// Everything else stays plain, so snippets in any language render the same way.

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Wraps double-quoted strings (with backslash escapes) in a span; the rest is escaped as is. */
function tintStrings(s: string): string {
  let out = "";
  let last = 0;
  for (const m of s.matchAll(/"(?:[^"\\]|\\.)*"/g)) {
    out += esc(s.slice(last, m.index)) + `<span class="s">${esc(m[0])}</span>`;
    last = m.index! + m[0].length;
  }
  return out + esc(s.slice(last));
}

export function codeToHtml(code: string): string {
  return code
    .replace(/^\n+|\s+$/g, "")
    .split("\n")
    .map((line) => {
      if (/^\s*(#|\/\/)/.test(line)) return `<span class="o">${esc(line)}</span>`;
      const m = line.match(/^(.*?\S)(\s{2,}(?:#|\/\/)\s.*)$/);
      if (m) return `${tintStrings(m[1])}<span class="o">${esc(m[2])}</span>`;
      return tintStrings(line);
    })
    .join("\n");
}
