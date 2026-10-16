// TextMate grammars Shiki has no built-in for: redis-cli command lines, and a terminal session
// that mixes shell commands ("$ ") with redis-cli prompts ("6379> ") and their replies.
import type { LanguageRegistration } from "shiki";

export const redis = {
  name: "redis",
  scopeName: "source.redis",
  patterns: [{ include: "#line" }],
  repository: {
    line: {
      patterns: [
        { match: "^\\s*#.*$", name: "comment.line.number-sign.redis" },
        { match: "(?<=\\S)\\s{2,}#.*$", name: "comment.line.number-sign.redis" },
        { match: "^\\s*([A-Za-z][A-Za-z.]*)\\b", captures: { 1: { name: "keyword.control.command.redis" } } },
        { include: "#args" },
      ],
    },
    args: {
      patterns: [
        { begin: '"', end: '"', name: "string.quoted.double.redis", patterns: [{ match: "\\\\.", name: "constant.character.escape.redis" }] },
        { begin: "'", end: "'", name: "string.quoted.single.redis" },
        { match: "<[A-Za-z_]+>", name: "variable.parameter.placeholder.redis" },
        { match: "(?<=\\s)-?\\d+(?:\\.\\d+)?(?=\\s|$)", name: "constant.numeric.redis" },
        { match: "(?<=\\s)[A-Z][A-Z0-9_]+(?=\\s|$)", name: "storage.modifier.option.redis" },
        { match: "(?<=\\s)[$*>](?=\\s|$)", name: "keyword.operator.redis" },
      ],
    },
  },
} as unknown as LanguageRegistration;

export const redisSession = {
  name: "redis-session",
  scopeName: "source.redis-session",
  embeddedLangs: ["shellscript"],
  patterns: [
    {
      // "$ command", continued on the indented lines that follow it
      begin: "^(\\$ )",
      while: "^(?=\\s)",
      beginCaptures: { 1: { name: "punctuation.definition.prompt.redis-session" } },
      patterns: [{ include: "source.shell" }],
    },
    {
      begin: "^(\\S*\\d+> )",
      end: "$",
      beginCaptures: { 1: { name: "punctuation.definition.prompt.redis-session" } },
      patterns: [{ include: "source.redis#line" }],
    },
    { match: "^.+$", name: "comment.line.output.redis-session" },
  ],
} as unknown as LanguageRegistration;
