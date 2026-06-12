export function producerStateLabel(state: string): string {
  const base = state.replace(/\s+assign$/, "").trim();
  switch (base) {
    case "idle":
      return "idle";
    case "ready":
      return "ready";
    case "busy":
      return state.includes("assign") ? "ASGN" : "busy";
    default:
      return base;
  }
}

export function producerStateClass(state: string): string {
  const base = state.replace(/\s+assign$/, "").trim();
  return state.includes("assign")
    ? `state-${base} state-assign`
    : `state-${base}`;
}
