#!/usr/bin/env python3
"""Prints the client methods a new command needs, one skeleton per client.

    python3 scripts/client_stubs.py HSTRLEN --reply integer
    python3 scripts/client_stubs.py LCS --reply bulk --args key1 key2

The command must already be in clients/testdata/commands.json (add the table row and
regenerate it first, see docs/DEVELOPING.md). Arguments default to the command's arity,
with its key positions named `key`, `key2`, ...; pass --args to name them yourself.
A command with a variable argument count gets a last `rest` argument: shape it by hand.
The skeletons are a start, not the end: each client's idiom for options, overloads and
names (C# `...Async`, Go `ctx` first) still needs a person. Cluster routing needs nothing;
gen_client_keys.py already gave every client the key rule.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "clients" / "testdata" / "commands.json"

REPLIES = {
    # reply: (Rust type and helper, Go type and helper, Python converter, Node transform,
    #         Java type and reply, C# type and helper)
    "integer": ("i64", "integer", "int64", "intR", "None", "null", "Long", "Reply.LONG", "long", "Integer"),
    "bulk": ("Option<String>", "bulk_or_null", "string, bool", "strNil", "None", "null", "V", "r(Reply.VALUE)", "string?", "BulkOrNull"),
    "ok": ("()", "ok", "", "ok", "None", "null", "Void", "Reply.OK", "", "Ok"),
    "array": ("Vec<String>", "strings", "[]string", "strs", "None", "null", "List<V>", "r(Reply.VALUE_LIST)", "IReadOnlyList<string>", "ArrayOfBulks"),
}


def key_positions(rule, count):
    kind = rule["kind"]
    if kind == "range":
        last = rule["last"] if rule["last"] > 0 else count + rule["last"]
        return set(range(rule["first"], last + 1, rule["step"] or 1))
    if kind == "key_and_store":
        return {rule["first"]}
    return set()


def snake(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def camel(name):
    parts = snake(name).split("_")
    return parts[0] + "".join(part.title() for part in parts[1:])


def pascal(name):
    return "".join(part.title() for part in snake(name).split("_"))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("name")
    parser.add_argument("--reply", choices=sorted(REPLIES), default="integer")
    parser.add_argument("--args", nargs="*")
    options = parser.parse_args()

    spec = json.loads(SPEC.read_text())
    entry = next((c for c in spec["commands"] if c["name"] == options.name.upper()), None)
    if entry is None:
        print(f"{options.name.upper()} is not in clients/testdata/commands.json; add the table row and "
              "run RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec first", file=sys.stderr)
        return 1

    arity = entry["arity"]
    count = abs(arity)
    if options.args is not None:
        names = options.args
    else:
        keys = key_positions(entry["keys"], count)
        names, key_number, arg_number = [], 0, 0
        for position in range(1, count):
            if position in keys:
                key_number += 1
                names.append("key" if key_number == 1 else f"key{key_number}")
            else:
                arg_number += 1
                names.append(f"arg{arg_number}")
        if arity < 0:
            names.append("rest")
    variable = arity < 0 and options.args is None

    word = entry["name"]
    rust_t, rust_h, go_t, go_h, py_conv, node_tf, java_t, java_r, cs_t, cs_h = REPLIES[options.reply]
    method = snake(word)
    go_ret = f"({go_t}, error)" if go_t else "error"
    cs_ret = f"ValueTask<{cs_t}>" if cs_t else "ValueTask"
    token = re.sub(r"[^A-Z0-9]+", "_", word)

    rust_params = ", ".join(f"{n}: &[&str]" if n == "rest" else f"{n}: &str" for n in names)
    rust_args = ", ".join([json.dumps(word)] + [n for n in names if n != "rest"])
    rust_body = (f"let mut arguments = vec![{rust_args}];\n        arguments.extend_from_slice(rest);\n        "
                 f"self.{rust_h}(arguments)") if variable else f"self.{rust_h}([{rust_args}])"
    go_params = ", ".join(f"{n} ...string" if n == "rest" else f"{n} string" for n in names)
    go_chain = "".join(f".s({n})" for n in names if n != "rest")
    fixed = [name for name in names if name != "rest"]
    go_size = f"{len(fixed) + 1}" + (" + len(rest)" if variable else "")
    go_rest = "\n\tfor _, a := range rest {\n\t\tr.s(a)\n\t}" if variable else ""
    py_params = ", ".join(f"*{n}" if n == "rest" else n for n in names)
    py_args = ", ".join(f"*{n}" if n == "rest" else n for n in names)
    node_params = ", ".join(f"...{n}: Arg[]" if n == "rest" else f"{n}: Arg" for n in names)
    node_args = ", ".join(f"...{n}" if n == "rest" else n for n in names)
    java_params = ", ".join(f"V... {n}" if n == "rest" else (f"K {n}" if n.startswith("key") else f"V {n}") for n in names)
    java_chain = "".join(f".k({n})" if n.startswith("key") else f".v({n})" for n in names if n != "rest")
    java_rest = f"\n        for (V item : rest) a.v(item);" if variable else ""
    cs_params = ", ".join(f"IEnumerable<string> {n}" if n == "rest" else f"string {n}" for n in names)

    row = {"rust": method, "csharp": f"{pascal(word)}Async", "go": pascal(word), "java": camel(word),
           "node": camel(word), "python": method}

    print(f"""{word}: arity {arity}, flags {', '.join(entry['flags']) or 'none'}, keys {json.dumps(entry['keys'])}
Reply shape assumed: {options.reply}. Add each method next to its family, then this one row to
clients/testdata/command_methods.json (keep it sorted by command), which every client's coverage test reads:
    {json.dumps(word)}: {json.dumps(row)},

--- Rust: clients/ruvio-client/src/commands.rs; tests/coverage.rs methods_exist()
    pub fn {method}(&mut self, {rust_params}) -> Result<{rust_t}, Error> {{
        {rust_body}
    }}

--- Go: clients/go/cmds_<family>.go; tokens.go c{token} = preEnc("{word}")
func (c *cmds) {pascal(word)}(ctx context.Context, {go_params}) {go_ret} {{
	r := newCmd({go_size}, c{token}){go_chain}{go_rest}
	return c.{go_h}(ctx, r)
}}

--- Python: clients/python/ruvio/_commands.py; a live test
    def {method}(self, {py_params}) -> Any:
        return self._c({py_conv}, "{word}", {py_args})

--- Node: clients/node/src/commands.ts; src/command-names.ts {token}: cmd('{word}')
  {camel(word)}({node_params}): R<unknown> {{ return this._v({node_tf}, C.{token}, [{node_args}]); }}

--- Java: clients/java/src/main/java/.../commands/<Family>Commands.java, then python3 clients/java/tools/generate.py
    public CompletableFuture<{java_t}> {camel(word)}({java_params}) {{
        Args a = args().w(Cmd.{token}){java_chain};{java_rest}
        return send(a, {java_r});
    }}

--- C#: clients/csharp/Ruvio.Client/Commands/Commands.cs and IRuvioClient.cs
    public {cs_ret} {pascal(word)}Async({cs_params}{', ' if names else ''}CancellationToken cancellationToken = default)
        => {cs_h}(new RespCommand("{word}"{', ' if names else ''}{', '.join(n for n in names)}), cancellationToken);
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
