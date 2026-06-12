const STATE_META: Record<string, { label: string; tip: string }> = {
  idle: {
    label: "idle",
    tip: "Connected — no active CRDY cycle.",
  },
  awaiting_read: {
    label: "await CRDY",
    tip: "Connected — waiting for first TCP CRDY (or retry after failure).",
  },
  matcher_queue: {
    label: "match queue",
    tip: "CRDY ack — in matcher queue; ASGN not sent yet.",
  },
  awaiting_batch: {
    label: "await DELV",
    tip: "CRDY ok + ASGN — BATC in transit; master awaits producer DELV (not the next CRDY).",
  },
  processing: {
    label: "processing",
    tip: "DELV ok — consumer processing locally; next CRDY = ready again.",
  },
  delivery_failed: {
    label: "BATC failed",
    tip: "Last TCP delivery failed — consumer resends CRDY after timeout.",
  },
};

function stateKey(state: string): string {
  return state.replace(/\s+assign$/, "").trim();
}

export function consumerStateLabel(state: string): string {
  const key = stateKey(state);
  const assign = state.includes("assign");
  const label = STATE_META[key]?.label ?? key;
  return assign && key === "awaiting_batch" ? "ASGN" : label;
}

export function consumerStateTip(state: string): string {
  const key = stateKey(state);
  return STATE_META[key]?.tip ?? state;
}

export function consumerStateClass(state: string): string {
  const base = stateKey(state);
  return state.includes("assign")
    ? `state-${base} state-assign`
    : `state-${base}`;
}
