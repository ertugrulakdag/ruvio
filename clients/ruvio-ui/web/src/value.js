export function scalar(value) {
  if (value == null || value.kind === "null") return value?.kind === "null" ? "(nil)" : "";
  if (value.kind === "integer") return String(value.value);
  if (value.kind === "bulk" || value.kind === "simple" || value.kind === "error") {
    return value.text ?? value.base64 ?? "";
  }
  return "";
}

function isScalar(value) {
  return !value || value.kind !== "array";
}

function isEntry(value) {
  return (
    value?.kind === "array" &&
    value.items.length === 2 &&
    isScalar(value.items[0]) &&
    value.items[1]?.kind === "array" &&
    value.items[1].items.every(isScalar)
  );
}

function pairsOf(items) {
  const rows = [];
  for (let index = 0; index < items.length; index += 2) {
    rows.push({ field: scalar(items[index]), value: scalar(items[index + 1]) });
  }
  return rows;
}

function entryOf(value) {
  return {
    id: scalar(value.items[0]),
    fields: pairsOf(value.items[1].items),
  };
}

import { documentOf } from "./json.js";

export function present(command, value, mode = "both") {
  const document = documentOf(command, value);
  if (document) return { type: "json", ...document };
  if (!value || value.kind !== "array") {
    return { type: "scalar", text: scalar(value), error: value?.kind === "error" };
  }

  const items = value.items;
  if (items.length === 0) return { type: "list", title: "Empty", rows: [] };

  if (items.every(isScalar)) {
    const pairCommands = new Set(["HGETALL", "ACL GETUSER", "ZRANGE", "ZREVRANGE"]);
    if (pairCommands.has(command) && items.length % 2 === 0 && (command !== "ZRANGE" || mode !== "members")) {
      const rows = pairsOf(items);
      if (command === "ZRANGE" && mode === "scores") {
        return { type: "list", title: "Scores", rows: rows.map((row) => row.value) };
      }
      if (command === "ZRANGE" && mode === "members") {
        return { type: "list", title: "Members", rows: rows.map((row) => row.field) };
      }
      return {
        type: "pairs",
        title: command === "HGETALL" || command === "ACL GETUSER" ? "Fields" : "Members",
        rows,
      };
    }
    return { type: "list", title: "Values", rows: items.map(scalar) };
  }

  if (
    command === "SCAN" &&
    items.length === 2 &&
    isScalar(items[0]) &&
    items[1]?.kind === "array" &&
    items[1].items.every(isScalar)
  ) {
    return { type: "scan", cursor: scalar(items[0]), rows: items[1].items.map(scalar) };
  }

  if (
    items.every(
      (item) =>
        item?.kind === "array" &&
        item.items.length === 2 &&
        isScalar(item.items[0]) &&
        item.items[1]?.kind === "array" &&
        item.items[1].items.every(isEntry),
    )
  ) {
    return {
      type: "streams",
      streams: items.map((item) => ({
        name: scalar(item.items[0]),
        entries: item.items[1].items.map(entryOf),
      })),
    };
  }

  if (items.every(isEntry)) {
    return { type: "entries", entries: items.map(entryOf) };
  }

  return { type: "tree", items };
}

function counted(count, singular) {
  const plural = singular === "entry" ? "entries" : `${singular}s`;
  return `${count} ${count === 1 ? singular : plural}`;
}

export function summarize(value) {
  const view = present("", value);
  if (view.type === "scalar") return view.text || "(empty)";
  if (view.type === "json") return Array.isArray(view.value) ? counted(view.value.length, "item") : counted(Object.keys(view.value).length, "key");
  if (view.type === "list") return view.rows.length ? counted(view.rows.length, "value") : "empty";
  if (view.type === "pairs") return counted(view.rows.length, "field");
  if (view.type === "scan") return `cursor ${view.cursor}, ${counted(view.rows.length, "key")}`;
  if (view.type === "entries") return counted(view.entries.length, "entry");
  if (view.type === "streams") {
    return view.streams
      .map((stream) => `${stream.name}: ${counted(stream.entries.length, "entry")}`)
      .join(", ");
  }
  if (view.type === "tree") return counted(view.items.length, "row");
  return "";
}
