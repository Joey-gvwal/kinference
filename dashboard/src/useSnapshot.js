// kinference — live data source.
// Replaces the prototype's window.Sim. Polls the in-process Rust server's
// /api/snapshot endpoint and returns the latest snapshot. The JSON shape is
// identical to what simulation.js::snapshot() returned.

import React from "react";

// The GAP BETWEEN REQUESTS, not a period. The loop schedules the next fetch
// once the previous one has settled, so at most one is ever in flight.
//
// `setInterval` was wrong here in a way that only shows up on a slow link: it
// fires regardless of whether the last request finished, so a 2s round-trip
// puts four in flight at once — each one adding load to the very connection
// that is already the bottleneck. Worse, responses can then land out of order
// and `setSnap` walks the UI BACKWARDS in time.
//
// 500ms because the server-side scrape runs at ~1Hz and the payload is small;
// on a fast link the effective cadence is unchanged, and on a slow one it
// degrades to "as often as the network allows" instead of piling up.
const POLL_MS = 500;

// After a failure, back off rather than hammering a server that is down or
// unreachable. Reset on the first success.
const BACKOFF_MS = 5000;

// Safe empty default so the very first render (before any fetch resolves)
// doesn't crash on missing fields.
const EMPTY_SNAPSHOT = {
  ts: 0,
  version: "",
  score: {
    rps: 0, inflight: 0, p50: 0, p95: 0, p99: 0, total: 0,
    rpsHist: [], inflightHist: [], p50Hist: [], p95Hist: [], p99Hist: [],
    okHist: [], degradedHist: [], errHist: [],
  },
  tokenize: {
    rps: 0, inflight: 0, p50: 0, p95: 0, p99: 0,
    errorRate: 0, total: 0, unavailableTotal: 0,
    rpsHist: [], inflightHist: [], p50Hist: [], p95Hist: [], p99Hist: [],
    errorRateHist: [],
    localMatchesTotal: 0, localDivergencesTotal: 0, localErrorsTotal: 0,
    localServedTotal: 0, httpFallbackTotal: 0,
  },
  workers: [],
  globalOverlapHist: [],
  recentCalls: [],
  ticksHistory: [],
  numBuckets: 10,
  index: {
    blocks: 0, workers: 0,
    eventsPerSec: 0, storedPerSec: 0, removedPerSec: 0, clearedPerSec: 0,
    blocksStoredPerSec: 0, blocksRemovedPerSec: 0,
    blocksHist: [], eventsHist: [],
  },
  engine: {
    prefixCacheHitRate: 0,
    prefixCacheQueriesTotal: 0,
  },
  // `Pick()` outcomes. The activity panel hides its chrome line while
  // `total === 0`, so a process that has taken no traffic adds no noise.
  picks: {
    total: 0,
    noOpinion: 0,
    notFound: 0,
    p50: 0, p95: 0, p99: 0,
  },
};

/// `live` false fetches ONCE and then stops.
///
/// The one fetch is not a detail: the footer shows `snap.version` on every
/// tab, so a page opened directly at `?tab=policy` would otherwise render a
/// blank identity forever. After that there is nothing to poll for — the
/// policy explorer reads its own endpoints, and a snapshot nothing renders is
/// pure cost on a slow connection.
export function useSnapshot(live = true) {
  const [snap, setSnap] = React.useState(EMPTY_SNAPSHOT);

  React.useEffect(() => {
    let cancelled = false;
    let timer = null;
    let controller = null;
    let fetched = false;

    const tick = async () => {
      // This tick is running, so nothing is scheduled. Clearing it here is
      // what makes `timer === null` mean "the loop is not running" — without
      // it the id of an already-fired timeout lingers, and `wake` would
      // decline to restart the loop after the tab was hidden, permanently.
      timer = null;
      if (cancelled) return;
      // A hidden tab renders nothing, so polling it is pure waste on a
      // metered or slow connection. Come back when it is shown; the
      // `visibilitychange` listener below wakes the loop immediately rather
      // than leaving it to the next scheduled tick.
      if (typeof document !== "undefined" && document.hidden && fetched) {
        return;
      }
      let delay = POLL_MS;
      controller = new AbortController();
      try {
        const res = await fetch("/api/snapshot", {
          cache: "no-store",
          signal: controller.signal,
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (!cancelled && data) setSnap(data);
        fetched = true;
      } catch (e) {
        if (e.name === "AbortError") return;
        // Keep the last good snapshot — a blank dashboard is worse than a
        // slightly stale one — and try again less often.
        delay = BACKOFF_MS;
        fetched = true;
      } finally {
        controller = null;
      }
      // Scheduled only AFTER the request settled, which is what bounds
      // concurrency to one. Stop here when this hook is not live: the single
      // fetch above has done its job.
      if (!cancelled && live) timer = setTimeout(tick, delay);
    };

    const wake = () => {
      if (!cancelled && live && !document.hidden && timer === null) tick();
    };
    document.addEventListener("visibilitychange", wake);

    tick();
    return () => {
      cancelled = true;
      document.removeEventListener("visibilitychange", wake);
      if (timer) clearTimeout(timer);
      // Drop a request still in flight rather than letting it resolve into a
      // setState on an unmounted component.
      if (controller) controller.abort();
    };
  }, [live]);

  return snap;
}
