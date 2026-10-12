// Buoyant Enterprise for Linkerd — Layout
// AppBar, Sidenav, Paper, CardHeader, Summary, Table, ThemeToggle, PageShell.

import React from "react";
import { IconButton, Select, StatusDot } from "./BuoyantPrimitives.jsx";
import logoUrl from "../assets/buoyant-enterprise-for-linkerd.svg";

const BL_EASE = "cubic-bezier(0.4, 0.0, 0.2, 1)";

// ─── AppBar ──────────────────────────────────────────────────────────────
// Left block (sidenav width) is navy with the logo. Right block spans the
// rest of the bar with the page title on the left and actions on the right.
export const AppBar = ({ title, sidenavWidth = 224, onToggleTheme, theme = "light", rightActions }) => {
  return (
    <header style={{
      position: "sticky", top: 0, zIndex: 10,
      display: "flex", height: 64,
      background: "var(--bg-appbar)",
      boxShadow: "var(--shadow-4)",
    }}>
      <div style={{
        width: sidenavWidth, height: "100%",
        display: "flex", alignItems: "center", padding: "0 24px",
        background: "var(--bg-appbar)",
        borderRight: "1px solid rgba(255,255,255,0.08)",
      }}>
        <img src={logoUrl}
             alt="Buoyant Enterprise for Linkerd"
             style={{ height: 32, width: "auto" }} />
      </div>
      <div style={{
        flex: 1, display: "flex", alignItems: "center",
        padding: "0 24px", background: "var(--bg-appbar-alt)",
      }}>
        <span style={{
          fontFamily: "var(--font-sans)", fontSize: 20, fontWeight: 500,
          color: "#fff", letterSpacing: "0.15px",
        }}>{title}</span>
        <div style={{ marginLeft: "auto", display: "flex", gap: 4, alignItems: "center" }}>
          {rightActions}
          <ThemeToggle theme={theme} onToggle={onToggleTheme} />
        </div>
      </div>
    </header>
  );
};

// ─── ThemeToggle ─────────────────────────────────────────────────────────
export const ThemeToggle = ({ theme, onToggle }) => (
  <button onClick={onToggle} aria-label={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
    style={{
      width: 40, height: 40, borderRadius: "var(--radius-full)",
      border: "1px solid rgba(255,255,255,0.24)",
      background: "transparent", cursor: "pointer",
      display: "inline-flex", alignItems: "center", justifyContent: "center",
      color: "#fff", transition: `background .15s ${BL_EASE}`,
    }}
    onMouseEnter={(e) => e.currentTarget.style.background = "rgba(255,255,255,0.08)"}
    onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}>
    <span className="material-icons-outlined" style={{ fontSize: 22 }}>
      {theme === "light" ? "dark_mode" : "light_mode"}
    </span>
  </button>
);

// ─── Sidenav ─────────────────────────────────────────────────────────────
// Structure mirrors the Figma exactly: subheader groups "Cluster" + "Application",
// with a divider above "Community", external-link row with a trailing `open_in_new`,
// and a version caption at the bottom.
export const Sidenav = ({ current, onNavigate, cluster = "production", onClusterChange }) => {
  const CLUSTERS = [
    { value: "production", label: "production" },
    { value: "staging",    label: "staging" },
    { value: "preview",    label: "preview" },
  ];
  return (
    <aside style={{
      width: 224, flexShrink: 0,
      position: "sticky", top: 64, alignSelf: "flex-start",
      height: "calc(100vh - 64px)",
      background: "var(--bg-sidenav)",
      borderRight: "1px solid var(--border)",
      display: "flex", flexDirection: "column",
      overflowY: "auto",
    }}>
      <div style={{ flex: 1, padding: "8px 0" }}>
        <Subheader>Cluster</Subheader>
        <NavItem icon="account_tree" label="Namespaces" active={current === "namespaces"} onClick={() => onNavigate?.("namespaces")} />
        <NavItem icon="lock_outline" label="mTLS" active={current === "mtls"} onClick={() => onNavigate?.("mtls")} />
        <NavItem icon="router" label="Gateway" active={current === "gateway"} onClick={() => onNavigate?.("gateway")} />

        <Subheader>Application</Subheader>
        <div style={{ padding: "8px 16px" }}>
          <Select label="Label" value={cluster} onChange={onClusterChange} options={CLUSTERS} size="small" fullWidth />
        </div>
        <NavItem icon="web_asset" label="Deployments" active={current === "deployments"} onClick={() => onNavigate?.("deployments")} />
        <NavItem icon="hive" label="Pods" active={current === "pods"} onClick={() => onNavigate?.("pods")} />
        <NavItem icon="keyboard_arrow_down" label="More" active={false} onClick={() => {}} />

        <div style={{ height: 1, background: "var(--divider)", margin: "8px 0" }} />
        <NavItem icon="newspaper" label="Community" active={current === "community"} onClick={() => onNavigate?.("community")} />
        <NavItem icon="description" label="Documentation" external />
        <NavItem icon="forum" label="Forums" external />
      </div>
      <div style={{
        padding: "16px", borderTop: "1px solid var(--border)",
        fontFamily: "var(--font-sans)", fontSize: 12, letterSpacing: "0.4px",
        color: "var(--fg-secondary)", display: "flex", alignItems: "center",
      }}>
        <span>Dashboard 0.7.0</span>
        <span style={{ flex: 1 }} />
        <IconButton icon="chevron_left" iconSize={18} size={32} variant="outlined" />
      </div>
    </aside>
  );
};

export const Subheader = ({ children }) => (
  <div style={{
    padding: "12px 16px 6px", fontFamily: "var(--font-sans)", fontSize: 11,
    fontWeight: 600, textTransform: "uppercase", letterSpacing: "1px",
    color: "var(--fg-secondary)",
  }}>{children}</div>
);

export const NavItem = ({ icon, label, active, onClick, external }) => {
  const [hover, setHover] = React.useState(false);
  return (
    <div onClick={onClick}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{
        display: "flex", alignItems: "center", gap: 16,
        padding: "8px 16px", height: 36, cursor: "pointer",
        background: active ? "var(--selection-bg)" : hover ? "var(--hover-bg)" : "transparent",
        borderLeft: active ? "3px solid var(--brand)" : "3px solid transparent",
        paddingLeft: 13,
        transition: `background .12s ${BL_EASE}`,
      }}>
      <span className="material-icons-outlined" style={{
        fontSize: 20, color: active ? "var(--brand)" : "var(--navy-a56)",
      }}>{icon}</span>
      <span style={{
        fontFamily: "var(--font-sans)", fontSize: 14, fontWeight: active ? 500 : 400,
        color: active ? "var(--brand)" : "var(--fg-primary)", flex: 1,
      }}>{label}</span>
      {external && (
        <span className="material-icons-outlined" style={{ fontSize: 16, color: "var(--navy-a60)" }}>open_in_new</span>
      )}
    </div>
  );
};

// ─── Paper ───────────────────────────────────────────────────────────────
export const Paper = ({ children, elevation = 1, style }) => (
  <div style={{
    background: "var(--bg-surface)", borderRadius: "var(--radius)",
    boxShadow: `var(--shadow-${elevation})`,
    border: "1px solid var(--border)",
    overflow: "hidden",
    ...style,
  }}>{children}</div>
);

// ─── CardHeader ──────────────────────────────────────────────────────────
export const CardHeader = ({ title, subtitle, action, style }) => (
  <div style={{
    display: "flex", alignItems: "center",
    padding: "16px 24px", minHeight: 64,
    ...style,
  }}>
    <div style={{ flex: 1 }}>
      <div style={{
        fontFamily: "var(--font-sans)", fontSize: 20, fontWeight: 500,
        letterSpacing: "0.15px", color: "var(--fg-primary)",
      }}>{title}</div>
      {subtitle && (
        <div style={{
          fontFamily: "var(--font-sans)", fontSize: 14, color: "var(--fg-secondary)",
          marginTop: 2,
        }}>{subtitle}</div>
      )}
    </div>
    {action && <div>{action}</div>}
  </div>
);

// ─── Summary row / cell ──────────────────────────────────────────────────
// The tinted navy region with big-number cells. Matches the Figma.
export const SummaryRow = ({ children, style }) => (
  <div style={{
    display: "flex", gap: 24, padding: 24,
    background: "var(--bg-summary)",
    borderTop: "1px solid var(--navy-a12)",
    ...style,
  }}>{children}</div>
);

export const SummaryCell = ({ value, label, status, statusLabel, style }) => (
  <div style={{
    flex: 1, background: "var(--bg-surface)", borderRadius: "var(--radius-md)",
    padding: 16, display: "flex", flexDirection: "column", gap: 4, ...style,
  }}>
    {status ? (
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <StatusDot status={status} />
        <span style={{ fontFamily: "var(--font-sans)", fontSize: 24, lineHeight: 1.334, color: "var(--fg-primary)" }}>
          {statusLabel || value}
        </span>
      </div>
    ) : (
      <span style={{ fontFamily: "var(--font-sans)", fontSize: 24, lineHeight: 1.334, color: "var(--fg-primary)" }}>{value}</span>
    )}
    <span style={{
      fontFamily: "var(--font-sans)", fontSize: 14, lineHeight: 1.43,
      letterSpacing: "0.17px", color: "var(--fg-secondary)",
    }}>{label}</span>
  </div>
);

// ─── Table ───────────────────────────────────────────────────────────────
// Custom table (no Data Grid). Columns: [{key, label, width, render?, align?}]
export const Table = ({ columns, rows, onRowClick, rowKey = "id", empty = "No rows", style }) => (
  <div style={{ overflowX: "auto", ...style }}>
    <table style={{
      width: "100%", borderCollapse: "collapse",
      fontFamily: "var(--font-sans)",
    }}>
      <thead>
        <tr>
          {columns.map(c => (
            <th key={c.key} style={{
              textAlign: c.align || "left", padding: "10px 24px",
              fontSize: 14, fontWeight: 500, color: "var(--fg-secondary)",
              borderBottom: "1px solid var(--divider)",
              width: c.width, whiteSpace: "nowrap",
            }}>{c.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr><td colSpan={columns.length} style={{
            padding: 32, textAlign: "center", color: "var(--fg-secondary)", fontSize: 14,
          }}>{empty}</td></tr>
        ) : rows.map((r, i) => (
          <TableRow key={r[rowKey] ?? i} row={r} columns={columns} onRowClick={onRowClick} />
        ))}
      </tbody>
    </table>
  </div>
);

export const TableRow = ({ row, columns, onRowClick }) => {
  const [hover, setHover] = React.useState(false);
  return (
    <tr onClick={() => onRowClick?.(row)}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{
        background: hover ? "var(--hover-bg)" : "transparent",
        cursor: onRowClick ? "pointer" : "default",
        transition: `background .12s ${BL_EASE}`,
      }}>
      {columns.map(c => (
        <td key={c.key} style={{
          padding: "14px 24px", fontSize: 14, color: "var(--fg-primary)",
          borderBottom: "1px solid var(--divider)",
          textAlign: c.align || "left", whiteSpace: "nowrap",
        }}>{c.render ? c.render(row) : row[c.key]}</td>
      ))}
    </tr>
  );
};

// ─── PageShell ───────────────────────────────────────────────────────────
export const PageShell = ({ children, style }) => (
  <main style={{
    flex: 1, padding: "24px 24px 48px",
    display: "flex", flexDirection: "column", gap: 24,
    minWidth: 0, background: "var(--bg-canvas)",
    ...style,
  }}>{children}</main>
);
