// Buoyant Enterprise for Linkerd — Primitives
// All styling reads from CSS variables in colors_and_type.css, so light/dark
// mode just works when [data-theme] flips on <html>.

import React from "react";

const BP_EASE = "cubic-bezier(0.4, 0.0, 0.2, 1)";

// ─── Button ──────────────────────────────────────────────────────────────
export const Button = ({
  children, variant = "contained", color = "primary", size = "medium",
  startIcon, endIcon, onClick, disabled, fullWidth, type = "button", style,
}) => {
  const [hover, setHover] = React.useState(false);
  const [press, setPress] = React.useState(false);
  const pad = size === "small" ? "4px 10px" : size === "large" ? "8px 22px" : "6px 16px";
  const fs  = size === "small" ? 13 : size === "large" ? 15 : 14;

  // Color resolution — "primary" = brand navy, "success/warning/error" = status
  const palette = {
    primary: { bg: "var(--brand)", hov: "var(--brand-hover)", fg: "var(--brand-contrast)",
               soft: "var(--hover-bg)", border: "var(--border-strong)" },
    success: { bg: "var(--bel-success)", hov: "var(--bel-success-dark)", fg: "#fff",
               soft: "var(--status-ok-bg)", border: "rgba(76,175,80,0.5)" },
    warning: { bg: "var(--bel-warning)", hov: "var(--bel-warning-dark)", fg: "#fff",
               soft: "var(--status-warn-bg)", border: "rgba(255,152,0,0.5)" },
    error:   { bg: "var(--bel-error)",   hov: "var(--bel-error-dark)",   fg: "#fff",
               soft: "var(--status-error-bg)", border: "rgba(211,47,47,0.5)" },
    inherit: { bg: "var(--fg-primary)",  hov: "var(--fg-primary)",       fg: "var(--bg-surface)",
               soft: "var(--hover-bg)",  border: "var(--border-strong)" },
  }[color] || {};

  let bg, fg, border = "1px solid transparent", shadow = "none";
  if (disabled) {
    bg = variant === "contained" ? "var(--navy-a12)" : "transparent";
    fg = "var(--fg-disabled)";
    if (variant === "outlined") border = "1px solid var(--border)";
  } else if (variant === "contained") {
    bg = hover ? palette.hov : palette.bg;
    fg = palette.fg;
    shadow = hover ? "var(--shadow-4)" : "var(--shadow-2)";
  } else if (variant === "outlined") {
    bg = hover ? palette.soft : "transparent";
    fg = palette.bg;
    border = `1px solid ${hover ? palette.bg : palette.border}`;
  } else { // text
    bg = hover ? palette.soft : "transparent";
    fg = palette.bg;
  }

  return (
    <button type={type} disabled={disabled} onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => { setHover(false); setPress(false); }}
      onMouseDown={() => setPress(true)}
      onMouseUp={() => setPress(false)}
      style={{
        fontFamily: "var(--font-sans)", fontWeight: 500, fontSize: fs, lineHeight: 1.75,
        letterSpacing: "0.4px", textTransform: "uppercase",
        padding: pad, borderRadius: "var(--radius)", border,
        background: bg, color: fg, boxShadow: shadow,
        cursor: disabled ? "default" : "pointer",
        display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 8,
        width: fullWidth ? "100%" : "auto", minWidth: 64,
        transition: `background-color .2s ${BP_EASE}, box-shadow .2s ${BP_EASE}, border-color .2s ${BP_EASE}`,
        ...style,
      }}>
      {startIcon && <span className="material-icons-outlined" style={{ fontSize: fs + 4 }}>{startIcon}</span>}
      {children}
      {endIcon && <span className="material-icons-outlined" style={{ fontSize: fs + 4 }}>{endIcon}</span>}
    </button>
  );
};

// ─── IconButton ──────────────────────────────────────────────────────────
export const IconButton = ({ icon, onClick, size = 40, iconSize = 22, color = "var(--navy-a56)", title, style, variant = "filled" /* filled|outlined */ }) => {
  const [hover, setHover] = React.useState(false);
  return (
    <button onClick={onClick} title={title}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{
        width: size, height: size, borderRadius: "var(--radius-full)",
        border: "none", background: hover ? "var(--hover-bg)" : "transparent",
        color, cursor: "pointer",
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        transition: `background-color .15s ${BP_EASE}`, padding: 0, ...style,
      }}>
      <span className={variant === "outlined" ? "material-icons-outlined" : "material-icons"} style={{ fontSize: iconSize }}>{icon}</span>
    </button>
  );
};

// ─── TextField ───────────────────────────────────────────────────────────
export const TextField = ({
  label, value, onChange, placeholder, type = "text", helperText, error, disabled,
  startAdornment, endAdornment, size = "medium", fullWidth, style, onFocus, onBlur, onKeyDown, inputRef,
}) => {
  const [focus, setFocus] = React.useState(false);
  const filled = !!value || !!placeholder || focus;
  const h = size === "small" ? 40 : 56;
  const borderColor = error ? "var(--bel-error)" : focus ? "var(--border-focus)" : "var(--border-strong)";
  return (
    <div style={{ width: fullWidth ? "100%" : "auto", ...style }}>
      <div style={{
        position: "relative", height: h,
        display: "flex", alignItems: "center",
        borderRadius: "var(--radius)",
        border: `${focus ? 2 : 1}px solid ${borderColor}`,
        padding: `0 ${focus ? 13 : 14}px`,
        background: disabled ? "var(--navy-a04)" : "var(--bg-surface)",
        transition: `border-color .15s ${BP_EASE}`,
      }}>
        {label && (
          <label style={{
            position: "absolute", left: filled ? 14 : 14,
            top: filled ? -8 : "50%", transform: filled ? "none" : "translateY(-50%)",
            fontFamily: "var(--font-sans)", fontSize: filled ? 12 : 16,
            color: error ? "var(--bel-error)" : focus ? "var(--border-focus)" : "var(--fg-secondary)",
            background: filled ? "var(--bg-surface)" : "transparent", padding: filled ? "0 4px" : 0,
            pointerEvents: "none",
            transition: `all .15s ${BP_EASE}`,
          }}>{label}</label>
        )}
        {startAdornment && <span style={{ marginRight: 8, color: "var(--fg-secondary)", display: "flex" }}>{startAdornment}</span>}
        <input
          ref={inputRef} type={type} value={value ?? ""} placeholder={focus ? placeholder : ""}
          onChange={(e) => onChange?.(e.target.value)}
          onFocus={(e) => { setFocus(true); onFocus?.(e); }}
          onBlur={(e) => { setFocus(false); onBlur?.(e); }}
          onKeyDown={onKeyDown} disabled={disabled}
          style={{
            flex: 1, border: "none", outline: "none", background: "transparent",
            fontFamily: "var(--font-sans)", fontSize: 16, color: "var(--fg-primary)",
            height: "100%", padding: 0, minWidth: 0,
          }}
        />
        {endAdornment && <span style={{ marginLeft: 8, color: "var(--fg-secondary)", display: "flex" }}>{endAdornment}</span>}
      </div>
      {helperText && (
        <div style={{
          fontFamily: "var(--font-sans)", fontSize: 12, lineHeight: 1.66, letterSpacing: "0.4px",
          color: error ? "var(--bel-error)" : "var(--fg-secondary)",
          marginTop: 4, padding: "0 14px",
        }}>{helperText}</div>
      )}
    </div>
  );
};

// ─── Select (simple, click-to-open) ──────────────────────────────────────
export const Select = ({ label, value, onChange, options, size = "medium", fullWidth, style, startAdornment }) => {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h);
  }, []);
  const selected = options.find(o => o.value === value);
  return (
    <div ref={ref} style={{ position: "relative", width: fullWidth ? "100%" : "auto", ...style }}>
      <div onClick={() => setOpen(o => !o)} style={{
        height: size === "small" ? 40 : 56,
        display: "flex", alignItems: "center", gap: 8,
        padding: "0 14px",
        border: `${open ? 2 : 1}px solid ${open ? "var(--border-focus)" : "var(--border-strong)"}`,
        borderRadius: "var(--radius)", background: "var(--bg-surface)", cursor: "pointer",
      }}>
        {startAdornment && <span style={{ color: "var(--fg-secondary)", display: "flex" }}>{startAdornment}</span>}
        {label && !selected && (
          <span style={{ fontFamily: "var(--font-sans)", fontSize: 16, color: "var(--fg-secondary)" }}>{label}</span>
        )}
        {selected && (
          <span style={{ fontFamily: "var(--font-sans)", fontSize: 16, color: "var(--fg-primary)", flex: 1 }}>{selected.label}</span>
        )}
        <span className="material-icons" style={{ fontSize: 22, marginLeft: "auto", color: "var(--fg-secondary)", transform: open ? "rotate(180deg)" : "none", transition: `transform .15s ${BP_EASE}` }}>arrow_drop_down</span>
      </div>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 4px)", left: 0, right: 0,
          background: "var(--bg-surface)", borderRadius: "var(--radius)",
          boxShadow: "var(--shadow-8)", zIndex: 100, overflow: "hidden",
          border: "1px solid var(--border)",
        }}>
          {options.map(o => (
            <div key={o.value} onClick={() => { onChange?.(o.value); setOpen(false); }}
              style={{
                padding: "10px 16px", fontFamily: "var(--font-sans)", fontSize: 14,
                color: "var(--fg-primary)", cursor: "pointer",
                background: o.value === value ? "var(--selection-bg)" : "transparent",
              }}
              onMouseEnter={(e) => { if (o.value !== value) e.currentTarget.style.background = "var(--hover-bg)"; }}
              onMouseLeave={(e) => { if (o.value !== value) e.currentTarget.style.background = "transparent"; }}
            >{o.label}</div>
          ))}
        </div>
      )}
    </div>
  );
};

// ─── Checkbox ────────────────────────────────────────────────────────────
export const Checkbox = ({ checked, onChange, label, disabled, style }) => (
  <label style={{ display: "inline-flex", alignItems: "center", gap: 8, cursor: disabled ? "default" : "pointer", fontFamily: "var(--font-sans)", fontSize: 14, color: "var(--fg-primary)", ...style }}>
    <span onClick={() => !disabled && onChange?.(!checked)}
      style={{
        width: 18, height: 18, borderRadius: 2,
        border: checked ? "none" : "2px solid var(--navy-a54)",
        background: checked ? "var(--brand)" : "transparent",
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        transition: `all .15s ${BP_EASE}`,
      }}>
      {checked && <span className="material-icons" style={{ fontSize: 18, color: "var(--brand-contrast)" }}>check</span>}
    </span>
    {label && <span>{label}</span>}
  </label>
);

// ─── Switch ──────────────────────────────────────────────────────────────
export const Switch = ({ checked, onChange, disabled, style }) => (
  <span onClick={() => !disabled && onChange?.(!checked)}
    style={{
      display: "inline-flex", alignItems: "center",
      width: 38, height: 22, borderRadius: 11,
      background: checked ? "var(--brand)" : "var(--navy-a38)",
      padding: 2, cursor: disabled ? "default" : "pointer",
      transition: `background .2s ${BP_EASE}`, ...style,
    }}>
    <span style={{
      width: 18, height: 18, borderRadius: "50%", background: "#fff",
      boxShadow: "var(--shadow-1)", transform: checked ? "translateX(16px)" : "translateX(0)",
      transition: `transform .2s ${BP_EASE}`,
    }} />
  </span>
);

// ─── Chip ────────────────────────────────────────────────────────────────
export const Chip = ({ label, onDelete, onClick, color = "default", variant = "filled", size = "medium", icon, style }) => {
  const h = size === "small" ? 24 : 32;
  const palette = {
    default: { bg: "var(--navy-a08)", fg: "var(--fg-primary)", border: "var(--border)" },
    primary: { bg: "rgba(0,51,89,0.16)", fg: "var(--brand)", border: "var(--brand)" },
    success: { bg: "var(--status-ok-bg)", fg: "var(--status-ok-fg)", border: "var(--bel-success)" },
    warning: { bg: "var(--status-warn-bg)", fg: "var(--status-warn-fg)", border: "var(--bel-warning)" },
    error:   { bg: "var(--status-error-bg)", fg: "var(--status-error-fg)", border: "var(--bel-error)" },
  }[color];
  return (
    <span onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      height: h, padding: `0 ${onDelete ? 6 : 12}px 0 ${icon ? 6 : 12}px`,
      borderRadius: "var(--radius-pill)",
      background: variant === "outlined" ? "transparent" : palette.bg,
      color: palette.fg,
      border: variant === "outlined" ? `1px solid ${palette.border}` : "none",
      fontFamily: "var(--font-sans)", fontSize: size === "small" ? 12 : 13, fontWeight: 400,
      cursor: onClick ? "pointer" : "default", ...style,
    }}>
      {icon && <span className="material-icons-outlined" style={{ fontSize: size === "small" ? 14 : 18 }}>{icon}</span>}
      {label}
      {onDelete && <span className="material-icons" onClick={(e) => { e.stopPropagation(); onDelete(); }}
        style={{ fontSize: size === "small" ? 14 : 18, cursor: "pointer", opacity: 0.7 }}>cancel</span>}
    </span>
  );
};

// ─── StatusDot ───────────────────────────────────────────────────────────
// The canonical pod-status glyph from the Figma: a colored dot before text.
export const StatusDot = ({ status = "ok", size = 8, style }) => {
  const colors = {
    ok: "var(--bel-success)",
    running: "var(--bel-success)",
    warning: "var(--bel-warning)",
    pending: "var(--bel-warning)",
    error: "var(--bel-error)",
    failed: "var(--bel-error)",
    unknown: "var(--navy-a38)",
  };
  return (
    <span style={{
      display: "inline-block", width: size, height: size, borderRadius: "50%",
      background: colors[status] || "var(--navy-a38)", flexShrink: 0, ...style,
    }} />
  );
};

// ─── Alert ───────────────────────────────────────────────────────────────
export const Alert = ({ severity = "info", title, children, onClose, action, variant = "standard" /* standard|outlined|filled */, style }) => {
  const icons = { success: "check_circle", warning: "warning", error: "error", info: "info" };
  const palette = {
    success: { bg: "var(--status-ok-bg)", fg: "var(--status-ok-fg)", icon: "var(--bel-success)", full: "var(--bel-success)" },
    warning: { bg: "var(--status-warn-bg)", fg: "var(--status-warn-fg)", icon: "var(--bel-warning)", full: "var(--bel-warning)" },
    error:   { bg: "var(--status-error-bg)", fg: "var(--status-error-fg)", icon: "var(--bel-error)", full: "var(--bel-error)" },
    info:    { bg: "rgba(2,136,209,0.12)", fg: "var(--bel-info-dark)", icon: "var(--bel-info)", full: "var(--bel-info)" },
  }[severity];
  const isFilled = variant === "filled";
  return (
    <div role="alert" style={{
      display: "flex", gap: 12, padding: "6px 16px",
      background: isFilled ? palette.full : palette.bg,
      color: isFilled ? "#fff" : palette.fg,
      border: variant === "outlined" ? `1px solid ${palette.full}` : "none",
      borderRadius: "var(--radius)", alignItems: "flex-start", ...style,
    }}>
      <span className="material-icons-outlined" style={{ fontSize: 22, color: isFilled ? "#fff" : palette.icon, marginTop: 7 }}>{icons[severity]}</span>
      <div style={{ flex: 1, padding: "8px 0" }}>
        {title && <div style={{ fontFamily: "var(--font-sans)", fontSize: 16, fontWeight: 500, marginBottom: 2 }}>{title}</div>}
        <div style={{ fontFamily: "var(--font-sans)", fontSize: 14, lineHeight: 1.43 }}>{children}</div>
      </div>
      {action && <div style={{ marginLeft: "auto", alignSelf: "center" }}>{action}</div>}
      {onClose && !action && (
        <IconButton icon="close" iconSize={18} size={30} onClick={onClose}
          color={isFilled ? "#fff" : palette.fg} style={{ alignSelf: "flex-start", marginTop: 4 }} />
      )}
    </div>
  );
};

// ─── DatePicker (MUI X-style lightweight) ───────────────────────────────
// Supports single-date picking; opens a calendar popover. Day-grid navigates
// month by month. Value is a Date | null.
export const DatePicker = ({ label, value, onChange, size = "medium", fullWidth, style }) => {
  const [open, setOpen] = React.useState(false);
  const [view, setView] = React.useState(() => value || new Date());
  const ref = React.useRef(null);

  React.useEffect(() => {
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  const fmt = (d) => d ? `${String(d.getMonth()+1).padStart(2,"0")}/${String(d.getDate()).padStart(2,"0")}/${d.getFullYear()}` : "";
  const MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"];
  const DAYS = ["S","M","T","W","T","F","S"];

  const y = view.getFullYear(), m = view.getMonth();
  const firstDay = new Date(y, m, 1).getDay();
  const daysInMonth = new Date(y, m+1, 0).getDate();
  const today = new Date(); today.setHours(0,0,0,0);

  const selDay = value ? (value.getFullYear() === y && value.getMonth() === m ? value.getDate() : null) : null;

  return (
    <div ref={ref} style={{ position: "relative", width: fullWidth ? "100%" : "auto", ...style }}>
      <TextField
        label={label} value={fmt(value)} onChange={() => {}} size={size} fullWidth={fullWidth}
        endAdornment={<IconButton icon="calendar_today" iconSize={18} size={28} onClick={() => setOpen(o => !o)} />}
        inputRef={null}
        style={{ cursor: "pointer" }}
      />
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 6px)", left: 0,
          background: "var(--bg-surface)", borderRadius: "var(--radius)",
          boxShadow: "var(--shadow-8)", padding: 16, zIndex: 120,
          border: "1px solid var(--border)", width: 320,
        }}>
          <div style={{ display: "flex", alignItems: "center", marginBottom: 12 }}>
            <IconButton icon="chevron_left" iconSize={20} size={32} onClick={() => setView(new Date(y, m-1, 1))} />
            <div style={{ flex: 1, textAlign: "center", fontFamily: "var(--font-sans)", fontSize: 16, fontWeight: 500, color: "var(--fg-primary)" }}>
              {MONTHS[m]} {y}
            </div>
            <IconButton icon="chevron_right" iconSize={20} size={32} onClick={() => setView(new Date(y, m+1, 1))} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 2 }}>
            {DAYS.map((d, i) => (
              <div key={i} style={{ textAlign: "center", fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", padding: "6px 0" }}>{d}</div>
            ))}
            {Array.from({ length: firstDay }).map((_, i) => <div key={`e${i}`} />)}
            {Array.from({ length: daysInMonth }).map((_, i) => {
              const day = i + 1;
              const isSel = day === selDay;
              const isToday = y === today.getFullYear() && m === today.getMonth() && day === today.getDate();
              return (
                <div key={day} onClick={() => { onChange?.(new Date(y, m, day)); setOpen(false); }}
                  style={{
                    aspectRatio: "1", display: "flex", alignItems: "center", justifyContent: "center",
                    borderRadius: "50%", cursor: "pointer",
                    fontFamily: "var(--font-sans)", fontSize: 13,
                    background: isSel ? "var(--brand)" : "transparent",
                    color: isSel ? "var(--brand-contrast)" : isToday ? "var(--brand)" : "var(--fg-primary)",
                    border: isToday && !isSel ? "1px solid var(--brand)" : "1px solid transparent",
                  }}
                  onMouseEnter={(e) => { if (!isSel) e.currentTarget.style.background = "var(--hover-bg)"; }}
                  onMouseLeave={(e) => { if (!isSel) e.currentTarget.style.background = "transparent"; }}
                >{day}</div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};
