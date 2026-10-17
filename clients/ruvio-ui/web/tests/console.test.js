import { test } from "node:test";
import assert from "node:assert/strict";
import { tokenize, classify, explainError, formatValue, viewsFor } from "../src/console.js";

test("tokenize splits plain words and quoted strings", () => {
  assert.deepEqual(tokenize('SET key "hello world"').args, ["SET", "key", "hello world"]);
  assert.deepEqual(tokenize("  GET   k  ").args, ["GET", "k"]);
  assert.deepEqual(tokenize("SET k 'it\\'s'").args, ["SET", "k", "it's"]);
  assert.deepEqual(tokenize('SET k ""').args, ["SET", "k", ""]);
});

test("tokenize decodes escapes in double quotes only", () => {
  assert.deepEqual(tokenize('SET k "a\\nb\\x41\\"q"').args, ["SET", "k", 'a\nbA"q']);
  assert.deepEqual(tokenize("SET k 'a\\nb'").args, ["SET", "k", "a\\nb"]);
});

test("tokenize reports bad quoting", () => {
  assert.match(tokenize('SET k "open').error, /closing/);
  assert.match(tokenize('SET k "a"b').error, /space/);
  assert.deepEqual(tokenize("   ").args, []);
});

test("classify blocks streaming and blocking commands", () => {
  assert.equal(classify(["subscribe", "c"]).kind, "blocked");
  assert.equal(classify(["BLPOP", "l", "0"]).kind, "blocked");
  assert.equal(classify(["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]).kind, "blocked");
  assert.equal(classify(["XREAD", "STREAMS", "s", "0"]).kind, "ok");
  assert.equal(classify(["MULTI"]).kind, "blocked");
});

test("classify asks before destructive commands", () => {
  for (const args of [["FLUSHALL"], ["flushdb"], ["KEYS", "*"], ["CONFIG", "SET", "a", "b"], ["ACL", "DELUSER", "x"]]) {
    assert.equal(classify(args).kind, "confirm", args.join(" "));
  }
  assert.equal(classify(["GET", "k"]).kind, "ok");
  assert.equal(classify(["CONFIG", "GET", "*"]).kind, "ok");
});

test("explainError keeps the raw text and adds a hint when known", () => {
  assert.match(explainError("WRONGTYPE Operation against a key").hint, /TYPE/);
  assert.match(explainError("NOPERM this user has no permissions").hint, /ACL/);
  assert.equal(explainError("something odd").hint, "");
  assert.equal(explainError("something odd").text, "something odd");
});

const bulk = (text) => ({ kind: "bulk", text });

test("formatValue renders scalars redis-cli style", () => {
  assert.equal(formatValue({ kind: "integer", value: 7 }).lines[0].text, "(integer) 7");
  assert.equal(formatValue({ kind: "null" }).lines[0].text, "(nil)");
  assert.equal(formatValue({ kind: "simple", text: "OK" }).lines[0].text, "OK");
  assert.equal(formatValue(bulk("hi")).lines[0].text, '"hi"');
});

test("formatValue numbers array items and nests", () => {
  const out = formatValue({ kind: "array", items: [bulk("a"), { kind: "array", items: [bulk("b")] }] });
  assert.deepEqual(out.lines.map((line) => line.text), ['1) "a"', "2) array of 1", '   1) "b"']);
});

test("formatValue caps lines and long strings", () => {
  const items = Array.from({ length: 10_000 }, (_, index) => bulk(String(index)));
  const out = formatValue({ kind: "array", items }, { limit: 200 });
  assert.equal(out.lines.length, 200);
  assert.equal(out.more, true);
  assert.equal(out.total, 10_000);
  const long = formatValue(bulk("x".repeat(5000)), { string: 100 }).lines[0].text;
  assert.ok(long.length < 200);
  assert.match(long, /4,900 more characters hidden/);
});

test("formatValue marks binary replies instead of dumping them", () => {
  const out = formatValue({ kind: "bulk", base64: "A".repeat(400) });
  assert.match(out.lines[0].text, /^\(binary, about 300 bytes/);
});

test("formatValue shows a multi-line string as real lines in auto mode", () => {
  const out = formatValue(bulk("# Server\r\nversion:1\r\n\r\n# Stats\r\nx:2"));
  assert.deepEqual(out.lines.map((line) => line.text), ["# Server", "version:1", "", "# Stats", "x:2"]);
  assert.equal(out.mode, "text");
});

test("formatValue trims lines and drops blanks when asked", () => {
  const out = formatValue(bulk("  a  \n\n  b"), { view: "text", trim: true });
  assert.deepEqual(out.lines.map((line) => line.text), ["a", "b"]);
});

test("formatValue pretty-prints JSON replies and can show them quoted", () => {
  const raw = bulk('{"a":1,"b":[true,null]}');
  const auto = formatValue(raw);
  assert.equal(auto.mode, "json");
  assert.equal(auto.lines[0].text, "{");
  assert.ok(auto.lines.some((line) => line.text === '  "a": 1,'));
  assert.equal(formatValue(raw, { view: "quoted" }).lines[0].text, '"{\\"a\\":1,\\"b\\":[true,null]}"');
});

test("formatValue leaves invalid JSON and short strings alone in auto mode", () => {
  assert.equal(formatValue(bulk("{not json")).lines[0].text, '"{not json"');
  assert.equal(formatValue(bulk("hello")).lines[0].text, '"hello"');
});

test("viewsFor offers only views that apply", () => {
  assert.deepEqual(viewsFor({ kind: "integer", value: 1 }).map((view) => view.id), ["auto", "quoted"]);
  assert.deepEqual(viewsFor(bulk("x")).map((view) => view.id), ["auto", "text", "quoted"]);
  assert.deepEqual(viewsFor(bulk("[1]")).map((view) => view.id), ["auto", "text", "json", "quoted"]);
});
