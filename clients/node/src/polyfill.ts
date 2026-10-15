// SPDX-License-Identifier: MIT
// Node 18/20 lack Symbol.asyncDispose; use the same registered symbol Node 22 and TypeScript use.
const S = Symbol as unknown as { asyncDispose?: symbol; dispose?: symbol };
if (S.asyncDispose === undefined) {
  Object.defineProperty(Symbol, 'asyncDispose', { value: Symbol.for('nodejs.asyncDispose') });
}
if (S.dispose === undefined) {
  Object.defineProperty(Symbol, 'dispose', { value: Symbol.for('nodejs.dispose') });
}
export {};
