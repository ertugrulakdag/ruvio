export function preference(name, fallback) {
  if (typeof localStorage === "undefined") return fallback;
  try {
    const value = localStorage.getItem(`ruvio.${name}`);
    return value === null ? fallback : JSON.parse(value);
  } catch (error) {
    console.warn(`Could not read ${name} preference`, error);
    return fallback;
  }
}

export function savePreference(name, value) {
  try {
    localStorage.setItem(`ruvio.${name}`, JSON.stringify(value));
  } catch (error) {
    console.warn(`Could not save ${name} preference`, error);
  }
}
