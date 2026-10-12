// kinference — main App
// Composes sidenav (chrome), topbar, KPI strip, panels and the drawer.

import React from "react";
import { useSnapshot } from "./useSnapshot.js";
import { IconButton } from "./BuoyantPrimitives.jsx";
import { Subheader, NavItem } from "./BuoyantLayout.jsx";
import {
  // `CallsTape` is intentionally not imported here — it's defined in
  // panels.jsx but unmounted in favor of `TokenizeActivityPanel` for
  // the right column of the activity row. Re-enable by re-importing
  // and swapping back in.
  KpiStrip, CacheIndexRow, ScoreActivityPanel, TokenizeActivityPanel,
  OverlapPanel, WorkersTable, WorkerDrawer,
} from "./panels.jsx";
import { useTweaks, TweaksPanel, TweakSection, TweakRadio } from "./tweaks-panel.jsx";
import { PolicyExplorer } from "./policy-explorer.jsx";
import logoUrl from "../assets/bel-dark.svg";

// ─── TWEAKS — declared at module scope so the host can rewrite ─
const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "theme": "dark",
  "vizStyle": "heatmap"
} /*EDITMODE-END*/;

// ─── topbar pill — time-window picker ──────────────────────────
const WindowPicker = ({ value, onChange }) => {
  const OPTIONS = [
  { v: 60, label: "60s" },
  { v: 300, label: "5m" },
  { v: 900, label: "15m" },
  { v: 3600, label: "1h" }];

  return (
    <div style={{
      display: "inline-flex", border: "1px solid rgba(255,255,255,0.20)",
      borderRadius: "var(--radius)", overflow: "hidden", height: 32, flexShrink: 0
    }}>
      {OPTIONS.map((o, i) =>
      <button key={o.v} onClick={() => onChange(o.v)}
      style={{
        border: "none", padding: "0 10px",
        background: value === o.v ? "rgba(255,255,255,0.16)" : "transparent",
        color: value === o.v ? "#fff" : "rgba(255,255,255,0.7)",
        fontFamily: "var(--font-sans)", fontSize: 12, fontWeight: 500,
        cursor: "pointer", letterSpacing: "0.4px",
        borderRight: i < OPTIONS.length - 1 ? "1px solid rgba(255,255,255,0.16)" : "none",
        minWidth: 38, whiteSpace: "nowrap"
      }}>
          {o.label}
        </button>
      )}
    </div>);

};

// The tab is URL state, not component state: `?tab=policy` is linkable, it
// survives a reload, and Back moves between tabs the way it does between pages.
// An unknown value falls back rather than blanking the page.
const ROUTES = ["overview", "policy"];
const routeFromUrl = () => {
  const t = new URLSearchParams(window.location.search).get("tab");
  return ROUTES.includes(t) ? t : "overview";
};

// ─── topbar pill — tab picker ──────────────────────────────────
// Same segmented control as `WindowPicker`. Deliberately NOT the sidenav:
// `SHOW_CHROME` is false and `MonitorSidenav`'s other seven items have no
// route, so enabling it would make each of them blank the page.
const RoutePicker = ({ value, onChange }) => {
  const OPTIONS = [
  { v: "overview", label: "Monitor" },
  { v: "policy", label: "Policy" }];

  return (
    <div style={{
      display: "inline-flex", border: "1px solid rgba(255,255,255,0.20)",
      borderRadius: "var(--radius)", overflow: "hidden", height: 32, flexShrink: 0
    }}>
      {OPTIONS.map((o, i) =>
      <button key={o.v} onClick={() => onChange(o.v)}
      style={{
        border: "none", padding: "0 12px",
        background: value === o.v ? "rgba(255,255,255,0.16)" : "transparent",
        color: value === o.v ? "#fff" : "rgba(255,255,255,0.7)",
        fontFamily: "var(--font-sans)", fontSize: 12, fontWeight: 500,
        cursor: "pointer", letterSpacing: "0.4px",
        borderRight: i < OPTIONS.length - 1 ? "1px solid rgba(255,255,255,0.16)" : "none",
        whiteSpace: "nowrap"
      }}>
          {o.label}
        </button>
      )}
    </div>);

};

const LiveIndicator = () => {
  const [t, setT] = React.useState(0);
  React.useEffect(() => {
    const id = setInterval(() => setT((x) => x + 1), 1000);
    return () => clearInterval(id);
  }, []);
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: 8,
      padding: "0 12px", height: 32, borderRadius: "var(--radius)",
      border: "1px solid rgba(255,255,255,0.20)",
      fontFamily: "var(--font-sans)", fontSize: 12, color: "#fff", letterSpacing: "0.4px",
      whiteSpace: "nowrap", flexShrink: 0
    }}>
      <span style={{
        width: 8, height: 8, borderRadius: "50%",
        background: "var(--bel-link-green)",
        boxShadow: "0 0 8px var(--bel-link-green)",
        animation: "pulse 1.4s var(--ease-standard) infinite"
      }} />
      LIVE
    </div>);

};

// ─── Sidenav (kinference flavor) ──────────────────────────────
const MonitorSidenav = ({ current, onNavigate, version }) =>
<aside style={{
  width: 224, flexShrink: 0,
  position: "sticky", top: 64, alignSelf: "flex-start",
  height: "calc(100vh - 64px)",
  background: "var(--bg-sidenav)",
  borderRight: "1px solid var(--border)",
  display: "flex", flexDirection: "column",
  overflowY: "auto"
}}>
    <div style={{ flex: 1, padding: "8px 0" }}>
      <Subheader>Monitor</Subheader>
      <NavItem icon="dashboard" label="Overview" active={current === "overview"} onClick={() => onNavigate("overview")} />
      <NavItem icon="dns" label="Workers" active={current === "workers"} onClick={() => onNavigate("workers")} />
      <NavItem icon="memory" label="KV cache" active={current === "cache"} onClick={() => onNavigate("cache")} />
      <NavItem icon="account_tree" label="Routing" active={current === "routing"} onClick={() => onNavigate("routing")} />
      <NavItem icon="show_chart" label="score() calls" active={current === "scores"} onClick={() => onNavigate("scores")} />

      <Subheader>Telemetry</Subheader>
      <NavItem icon="leaderboard" label="Metrics" active={current === "metrics"} onClick={() => onNavigate("metrics")} />
      <NavItem icon="receipt_long" label="Logs" active={current === "logs"} onClick={() => onNavigate("logs")} />
      <NavItem icon="tune" label="Configuration" active={current === "config"} onClick={() => onNavigate("config")} />

      <div style={{ height: 1, background: "var(--divider)", margin: "8px 0" }} />
      <NavItem icon="description" label="Documentation" external />
      <NavItem icon="forum" label="Forums" external />
    </div>
    <div style={{
    padding: "16px", borderTop: "1px solid var(--border)",
    fontFamily: "var(--font-sans)", fontSize: 12, letterSpacing: "0.4px",
    color: "var(--fg-secondary)", display: "flex", alignItems: "center"
  }}>
      <span>Monitor {version}</span>
      <span style={{ flex: 1 }} />
      <IconButton icon="chevron_left" iconSize={18} size={32} variant="outlined" />
    </div>
  </aside>;


// ─── Tweaks ────────────────────────────────────────────────────
const MonitorTweaks = ({ tweaks, setTweak, open, onClose }) =>
<TweaksPanel title="Tweaks" open={open} onClose={onClose}>
    <TweakSection label="Theme">
      <TweakRadio label="Color mode" value={tweaks.theme} options={[
    { value: "light", label: "Light" },
    { value: "dark", label: "Dark" }]
    } onChange={(v) => setTweak("theme", v)} />
    </TweakSection>
    <TweakSection label="Overlap visualization">
      <TweakRadio label="Style" value={tweaks.vizStyle} options={[
    { value: "heatmap", label: "Heatmap" },
    { value: "stacked", label: "Stacked" },
    { value: "ridgeline", label: "Density" }]
    } onChange={(v) => setTweak("vizStyle", v)} />
      <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)",
      lineHeight: 1.5, marginTop: 8, padding: "0 12px" }}>
        Heatmap is precise (cell = call count). Stacked normalizes to row composition.
        Density emphasizes shape for spotting bimodal distributions.
      </div>
    </TweakSection>
  </TweaksPanel>;


// ─── App ───────────────────────────────────────────────────────
export default function App() {
  const [tweaks, setTweak] = useTweaks(TWEAK_DEFAULTS);

  const [route, setRoute] = React.useState(routeFromUrl);
  const [selectedWorker, setSelectedWorker] = React.useState(null);
  const [windowSecs, setWindowSecs] = React.useState(60);
  const [tweaksOpen, setTweaksOpen] = React.useState(false);

  // Poll only while the monitor is showing. The policy explorer reads its own
  // endpoints and renders nothing from the snapshot, so on a slow link the
  // poll is pure cost. `useSnapshot` still fetches ONCE when not live, which
  // is what keeps the footer's version populated on a direct `?tab=policy`
  // load.
  const snap = useSnapshot(route === "overview");

  React.useEffect(() => {
    document.documentElement.setAttribute("data-theme", tweaks.theme);
  }, [tweaks.theme]);

  // Both directions: a click writes the URL, Back/Forward writes the state.
  React.useEffect(() => {
    const onPop = () => setRoute(routeFromUrl());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const navigate = (next) => {
    setRoute(next);
    const url = new URL(window.location);
    // The default tab leaves no param, so the bare URL stays the bare URL.
    if (next === "overview") url.searchParams.delete("tab");
    else url.searchParams.set("tab", next);
    window.history.pushState({}, "", url);
  };

  const openTweaks = () => setTweaksOpen((o) => !o);

  // Sidenav + appbar logo are hidden for now; flip to re-enable.
  const SHOW_CHROME = false;

  return (
    <div data-screen-label="Monitor / Overview" style={{ display: "flex", flexDirection: "column", minHeight: "100vh" }}>
      {/* AppBar */}
      <header style={{
        position: "sticky", top: 0, zIndex: 10,
        display: "flex", height: 64, background: "var(--bg-appbar)",
        boxShadow: "var(--shadow-4)"
      }}>
        {SHOW_CHROME && (
        <div style={{
          width: 224, height: "100%",
          display: "flex", alignItems: "center", padding: "0 20px",
          background: "var(--bg-appbar)",
          borderRight: "1px solid rgba(255,255,255,0.08)"
        }}>
          <img src={logoUrl}
          alt="Buoyant" style={{ height: 28, width: "auto" }} data-comment-anchor="34e4b07eeb-img-200-11" />
        </div>
        )}
        <div style={{
          flex: 1, display: "flex", alignItems: "center", gap: 14,
          padding: "0 24px", background: "var(--bg-appbar-alt)"
        }}>
          <span style={{
            fontFamily: "var(--font-sans)", fontSize: 18, fontWeight: 500,
            color: "#fff", letterSpacing: "0.15px", whiteSpace: "nowrap"
          }}>kinference</span>
          <RoutePicker value={route} onChange={navigate} />
          <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center", flexShrink: 0 }}>
            {/* The window and the live pulse describe the monitor's queries;
                the explorer evaluates a snapshot, so neither applies there. */}
            {route === "overview" && <LiveIndicator />}
            {route === "overview" &&
            <WindowPicker value={windowSecs} onChange={setWindowSecs} />}
            <IconButton icon="settings" iconSize={20} color="#fff"
            onClick={openTweaks}
            style={{ border: "1px solid rgba(255,255,255,0.20)", flexShrink: 0 }} />
          </div>
        </div>
      </header>

      <div style={{ display: "flex", flex: 1, minWidth: 0 }}>
        {SHOW_CHROME && <MonitorSidenav current={route} onNavigate={navigate} version={snap.version} />}

        <main style={{
          flex: 1, padding: "20px 24px 48px",
          display: "flex", flexDirection: "column", gap: 18,
          minWidth: 0, background: "var(--bg-canvas)"
        }}>
          {route === "policy" && <PolicyExplorer />}

          {route === "overview" && <>
          {/* KPI strip */}
          <KpiStrip snap={snap} window={windowSecs} />

          {/* Score activity + Calls tape */}
          <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1.55fr) minmax(0, 1fr)", gap: 18 }}>
            <ScoreActivityPanel snap={snap} window={windowSecs} />
            <TokenizeActivityPanel snap={snap} window={windowSecs} />
          </div>

          {/* Cache index (radix-tree health) */}
          <CacheIndexRow snap={snap} window={windowSecs} />

          {/* Overlap panel */}
          <OverlapPanel
            snap={snap}
            vizStyle={tweaks.vizStyle}
            window={windowSecs}
            onSelectWorker={setSelectedWorker} />


          {/* Workers table */}
          <WorkersTable snap={snap} onSelect={setSelectedWorker} window={windowSecs} />
          </>}

          <footer style={{
            display: "flex", justifyContent: "space-between",
            padding: "12px 4px", color: "var(--fg-secondary)",
            fontFamily: "var(--font-mono)", fontSize: 11
          }}>
            <span>kinference / {snap.version} · in-process dashboard</span>
            {route === "overview" &&
            <span>{snap.workers.length} workers · 1 Hz scrape · t={snap.ts}</span>}
          </footer>
        </main>
      </div>

      {/* Drawer */}
      {selectedWorker &&
      <WorkerDrawer
        snap={snap}
        workerId={selectedWorker}
        onClose={() => setSelectedWorker(null)}
        window={windowSecs} />

      }

      {/* Tweaks */}
      <MonitorTweaks tweaks={tweaks} setTweak={setTweak}
        open={tweaksOpen} onClose={() => setTweaksOpen(false)} />
    </div>);

}
