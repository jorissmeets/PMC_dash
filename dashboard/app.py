"""
Prinses Maxima Server Dashboard – Streamlit App
Starten: streamlit run app.py
"""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Configuratie ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
FILE1    = BASE_DIR / "vALL-pmc-vCenter.xlsx"
FILE2    = BASE_DIR / "Vcenter overzicht Prinses Maxima PPD - GK d.d. 01-04-2026 v1.xlsx"
NOW      = datetime.now()

BACKUP_WARN_DAYS = 2
REBOOT_WARN_DAYS = 90

TOOLS_LABELS = {
    "toolsOk":           "OK",
    "toolsOld":          "Verouderd",
    "toolsNotRunning":   "Niet actief",
    "toolsNotInstalled": "Niet geïnstalleerd",
    "toolsOnbekend":     "Onbekend",
}

# ─── Helpers ───────────────────────────────────────────────────────────────────
def parse_backup_date(text):
    if not text or not isinstance(text, str):
        return None
    m = re.search(r'\[(\d{1,2}-\d{1,2}-\d{4}\s+\d{2}:\d{2}:\d{2})\]', text)
    if m:
        try:
            return datetime.strptime(m.group(1), '%d-%m-%Y %H:%M:%S')
        except ValueError:
            return None
    return None


def parse_kernel_version(text):
    if not text or not isinstance(text, str):
        return None
    m = re.search(r"kernelVersion='([^']+)'", text)
    return m.group(1) if m else None


def days_since(dt):
    if dt is None or pd.isna(dt):
        return None
    try:
        return (NOW - dt.replace(tzinfo=None)).days
    except Exception:
        return None


def safe_dt(val):
    try:
        dt = val.to_pydatetime() if hasattr(val, 'to_pydatetime') else val
        return None if pd.isna(dt) else dt
    except Exception:
        return None


# ─── Data laden ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Data laden uit Excel-bestanden…")
def load_data():
    # ── Bestand 1 ──────────────────────────────────────────────────────────────
    df_info  = pd.read_excel(FILE1, sheet_name="vInfo",     engine="openpyxl")
    df_tools = pd.read_excel(FILE1, sheet_name="vTools",    engine="openpyxl")
    df_host  = pd.read_excel(FILE1, sheet_name="vHost",     engine="openpyxl")
    df_snap  = pd.read_excel(FILE1, sheet_name="vSnapshot", engine="openpyxl")

    df_info  = df_info[df_info["Template"] != True].copy()

    tools_map = {r["VM"]: r for _, r in df_tools.iterrows()}
    host_map  = {r["Host"]: r for _, r in df_host.iterrows()}
    snap_vms  = set(df_snap[df_snap["Name"] != "VEEAM BACKUP TEMPORARY SNAPSHOT"]["VM"].tolist())

    records = []
    for _, r in df_info.iterrows():
        vm    = r["VM"]
        tools = tools_map.get(vm, {})
        host  = host_map.get(r.get("Host", ""), {})

        backup_str   = str(r.get("Annotation", "") or "")
        backup_datum = parse_backup_date(backup_str)
        backup_flag  = str(r.get("Backup", "") or "").strip().capitalize()
        if backup_flag not in ("Ja", "Nee"):
            backup_flag = "Nee"
        if backup_datum and backup_flag == "Nee":
            backup_flag = "Ja"

        reboot_dt   = safe_dt(r.get("PowerOn"))
        host_boot   = safe_dt(host.get("Boot time") if isinstance(host, pd.Series) else None)

        records.append({
            "naam":              vm,
            "status":            r.get("Powerstate", ""),
            "locatie":           "PMC On-Premises",
            "datacenter":        str(r.get("Datacenter", "") or ""),
            "cluster":           str(r.get("Cluster", "") or ""),
            "esxi_host":         str(r.get("Host", "") or ""),
            "ip_adres":          str(r.get("Primary IP Address", "") or ""),
            "vcpu":              int(r.get("CPUs", 0) or 0),
            "geheugen_gib":      round(float(r.get("Memory", 0) or 0) / 1024, 1),
            "os":                str(r.get("OS according to the VMware Tools", "") or ""),
            "kernel_versie":     parse_kernel_version(str(r.get("Guest Detailed Data", "") or "")),
            "beheerder":         str(r.get("Beheerder", "") or ""),
            "afdeling":          str(r.get("Afdeling", "") or ""),
            "functie":           str(r.get("FunctieServer", "") or ""),
            "soort":             str(r.get("SoortServer", "") or ""),
            "sql_versie":        str(r.get("SQLVersion", "") or ""),
            "sql_editie":        str(r.get("SQLEdition", "") or ""),
            "update_schema":     str(r.get("Update", "") or ""),
            "update_moment":     str(r.get("Updatemoment", "") or ""),
            "klantnaam":         str(r.get("Klantnaam", "") or ""),
            "contract_nr":       str(r.get("AFASContractnummer", "") or ""),
            "ha_beschermd":      bool(r.get("DAS protection", False)),
            "aanmaakdatum":      str(r.get("Creation date", "") or ""),
            "wiki_link":         "",
            "backup_flag":       backup_flag,
            "backup_datum":      backup_datum,
            "backup_str":        backup_str[:200],
            "dagen_backup":      days_since(backup_datum),
            "laatste_reboot":    reboot_dt,
            "dagen_reboot":      days_since(reboot_dt),
            "host_boot":         host_boot,
            "dagen_host_boot":   days_since(host_boot),
            "esx_versie":        str(host.get("ESX Version", "") if isinstance(host, pd.Series) else ""),
            "tools_status":      str(tools.get("Tools", "") if isinstance(tools, pd.Series) else ""),
            "tools_versie":      str(tools.get("Tools Version", "") if isinstance(tools, pd.Series) else ""),
            "tools_upgradeable": str(tools.get("Upgradeable", "") if isinstance(tools, pd.Series) else ""),
            "heeft_snapshot":    vm in snap_vms,
            "bron":              "PMC",
            "alarmering":        str(r.get("Alarmering", "") or ""),
        })

    df1 = pd.DataFrame(records)

    # ── Bestand 2 ──────────────────────────────────────────────────────────────
    df2_raw = pd.read_excel(FILE2, sheet_name="Blad1", engine="openpyxl")
    df2_raw = df2_raw[df2_raw["Template"] != True].copy()

    records2 = []
    for _, r in df2_raw.iterrows():
        backup_str   = " ".join(filter(None, [str(r.get("backup", "") or ""), str(r.get("backup_details", "") or "")])).strip()
        backup_datum = parse_backup_date(backup_str)
        backup_flag  = "Ja" if backup_datum else "Nee"

        reboot_dt = safe_dt(r.get("PowerOn"))

        dc = str(r.get("Datacenter", "") or "")
        if "papend" in dc.lower() or "ppd" in dc.lower():
            locatie = "RAM DC – Papendorp"
        elif "groen" in dc.lower() or "gk" in dc.lower():
            locatie = "RAM DC – Groenekan"
        else:
            locatie = f"RAM DC – {dc}" if dc else "RAM DC"

        records2.append({
            "naam":              r.get("VM", ""),
            "status":            r.get("Powerstate", ""),
            "locatie":           locatie,
            "datacenter":        dc,
            "cluster":           str(r.get("Cluster", "") or ""),
            "esxi_host":         str(r.get("Host", "") or ""),
            "ip_adres":          str(r.get("Primary IP Address", "") or ""),
            "vcpu":              int(r.get("CPUs", 0) or 0),
            "geheugen_gib":      round(float(r.get("Memory", 0) or 0) / 1024, 1),
            "os":                str(r.get("OS according to the VMware Tools", "") or ""),
            "kernel_versie":     parse_kernel_version(str(r.get("Guest Detailed Data", "") or "")),
            "beheerder":         str(r.get("Beheerder", "") or ""),
            "afdeling":          str(r.get("Afdeling", "") or ""),
            "functie":           str(r.get("FunctieServer", "") or ""),
            "soort":             str(r.get("SoortServer", "") or ""),
            "sql_versie":        str(r.get("SQLVersion", "") or ""),
            "sql_editie":        str(r.get("SQLEdition", "") or ""),
            "update_schema":     str(r.get("Update", "") or ""),
            "update_moment":     str(r.get("Updatemoment", "") or ""),
            "klantnaam":         str(r.get("Klantnaam", "") or ""),
            "contract_nr":       str(r.get("AFASContractnummer", "") or ""),
            "ha_beschermd":      bool(r.get("DAS protection", False)),
            "aanmaakdatum":      str(r.get("Creation date", "") or ""),
            "wiki_link":         str(r.get("WikiLink", "") or ""),
            "backup_flag":       backup_flag,
            "backup_datum":      backup_datum,
            "backup_str":        backup_str[:200],
            "dagen_backup":      days_since(backup_datum),
            "laatste_reboot":    reboot_dt,
            "dagen_reboot":      days_since(reboot_dt),
            "host_boot":         None,
            "dagen_host_boot":   None,
            "esx_versie":        "",
            "tools_status":      "toolsOnbekend",
            "tools_versie":      "",
            "tools_upgradeable": "",
            "heeft_snapshot":    False,
            "bron":              "RAM-DC",
            "alarmering":        str(r.get("Alarmering", "") or ""),
        })

    df2 = pd.DataFrame(records2)
    return pd.concat([df1, df2], ignore_index=True)


# ─── Pagina configuratie ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Server Overzicht – Prinses Maxima",
    page_icon="🖥️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Login ─────────────────────────────────────────────────────────────────────
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown("## 🔐 Inloggen")
    st.markdown("Voer het wachtwoord in om het dashboard te openen.")
    pw = st.text_input("Wachtwoord", type="password", key="pw_input")
    if st.button("Inloggen", use_container_width=False):
        if pw == st.secrets.get("APP_PASSWORD", ""):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Onjuist wachtwoord.")
    return False

if not check_password():
    st.stop()

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700;800&display=swap');
  html, body, [class*="css"] { font-family: 'Open Sans', -apple-system, sans-serif !important; }
  [data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 800 !important; color: #2ea3f2 !important; }
  [data-testid="stMetricLabel"] { font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.5px; }
  .status-aan   { color: #276749; background: #c6f6d5; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
  .status-uit   { color: #9b2c2c; background: #fed7d7; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
  .status-susp  { color: #744210; background: #fefcbf; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
  .alert-box    { border-left: 4px solid #e53e3e; background: #fff5f5; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 8px; }
  .warn-box     { border-left: 4px solid #dd6b20; background: #fffbeb; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 8px; }
  .info-box     { border-left: 4px solid #2ea3f2; background: #ebf8ff; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 8px; }
  div[data-testid="stExpander"] summary { font-weight: 600; }
  [data-testid="stSidebar"] { background: #f8fafc; }
  .stButton > button { background: #2ea3f2 !important; color: white !important; border: none !important; font-family: 'Open Sans', sans-serif !important; font-weight: 600 !important; }
  .stButton > button:hover { background: #1a8fd1 !important; }
</style>
""", unsafe_allow_html=True)


# ─── Data laden ────────────────────────────────────────────────────────────────
df_all = load_data()


# ─── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#2ea3f2,#1a8fd1);padding:16px 14px;border-radius:10px;margin-bottom:8px">
      <div style="color:white;font-family:'Open Sans',sans-serif;font-size:22px;font-weight:800;letter-spacing:-0.5px">RAM IT</div>
      <div style="color:rgba(255,255,255,0.85);font-size:11px;margin-top:2px">Server Overzicht</div>
    </div>
    <div style="font-size:12px;color:#555;margin-bottom:4px">📍 Prinses Maxima Centrum</div>
    """, unsafe_allow_html=True)
    st.caption(f"Data geladen: {NOW.strftime('%d-%m-%Y %H:%M')}")

    if st.button("🔄 Data verversen", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("### Filters")

    locaties   = ["Alle"] + sorted(df_all["locatie"].dropna().unique().tolist())
    beheerders = ["Alle"] + sorted([b for b in df_all["beheerder"].dropna().unique().tolist() if b])
    soorten    = ["Alle"] + sorted([s for s in df_all["soort"].dropna().unique().tolist() if s])

    f_locatie   = st.selectbox("Locatie",     locaties)
    f_status    = st.selectbox("Status",      ["Alle", "Aan (poweredOn)", "Uit (poweredOff)", "Gesuspendeerd"])
    f_beheerder = st.selectbox("Beheerder",   beheerders)
    f_backup    = st.selectbox("Backup",      ["Alle", "Geconfigureerd", "Niet geconfigureerd"])
    f_soort     = st.selectbox("Soort server", soorten)
    f_search    = st.text_input("🔍 Zoeken", placeholder="VM-naam, IP, OS…")

    st.divider()
    st.markdown("### Weergave")
    show_details = st.checkbox("Detailpaneel tonen", value=True)
    show_alerts  = st.checkbox("Aandachtspunten tonen", value=True)


# ─── Filter toepassen ──────────────────────────────────────────────────────────
df = df_all.copy()

if f_locatie   != "Alle":
    df = df[df["locatie"] == f_locatie]
if f_status == "Aan (poweredOn)":
    df = df[df["status"] == "poweredOn"]
elif f_status == "Uit (poweredOff)":
    df = df[df["status"] == "poweredOff"]
elif f_status == "Gesuspendeerd":
    df = df[df["status"] == "suspended"]
if f_beheerder != "Alle":
    df = df[df["beheerder"] == f_beheerder]
if f_backup == "Geconfigureerd":
    df = df[df["backup_flag"] == "Ja"]
elif f_backup == "Niet geconfigureerd":
    df = df[df["backup_flag"] == "Nee"]
if f_soort != "Alle":
    df = df[df["soort"] == f_soort]
if f_search:
    mask = df[["naam","ip_adres","os","beheerder","functie","locatie"]].apply(
        lambda col: col.astype(str).str.contains(f_search, case=False, na=False)
    ).any(axis=1)
    df = df[mask]


# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="background:linear-gradient(135deg,#2ea3f2 0%,#1a8fd1 100%);padding:20px 28px;border-radius:12px;margin-bottom:20px;display:flex;align-items:center;justify-content:space-between">
  <div>
    <div style="color:white;font-family:'Open Sans',sans-serif;font-size:22px;font-weight:800;letter-spacing:-0.3px">Server Overzicht – Prinses Maxima Centrum</div>
    <div style="color:rgba(255,255,255,0.8);font-size:12px;margin-top:4px">VMware vCenter rapportage · {len(df)} van {len(df_all)} servers zichtbaar</div>
  </div>
  <div style="text-align:right;color:rgba(255,255,255,0.7);font-size:11px">
    <div style="font-size:20px;font-weight:900;color:white;letter-spacing:-1px">RAM IT</div>
    <div>Gegenereerd {NOW.strftime('%d-%m-%Y %H:%M')}</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ─── KPI Cards ────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
total    = len(df)
aan      = (df["status"] == "poweredOn").sum()
uit      = (df["status"] == "poweredOff").sum()
bk_ja    = (df["backup_flag"] == "Ja").sum()
bk_pct   = round(bk_ja / total * 100) if total else 0
tools_is = ((df["tools_status"] != "toolsOk") & (df["tools_status"] != "toolsOnbekend")).sum()

k1.metric("Totaal servers",     total)
k2.metric("Aan",                aan,   delta=f"{round(aan/total*100)}%" if total else None)
k3.metric("Uit",                uit,   delta=f"-{uit}" if uit else None, delta_color="inverse")
k4.metric("Backup geconfigureerd", f"{bk_pct}%", delta=f"{bk_ja}/{total}")
k5.metric("Tools aandacht",     tools_is, delta="OK" if tools_is == 0 else f"{tools_is} VMs", delta_color="inverse" if tools_is > 0 else "normal")


st.divider()


# ─── Grafieken ─────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

# Donut: Status
with col1:
    status_counts = df["status"].value_counts()
    label_map = {"poweredOn": "Aan", "poweredOff": "Uit", "suspended": "Gesuspendeerd"}
    fig = px.pie(
        values=status_counts.values,
        names=[label_map.get(s, s) for s in status_counts.index],
        title="Server Status",
        hole=0.55,
        color_discrete_sequence=["#48bb78", "#fc8181", "#fbd38d"],
    )
    fig.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                      legend=dict(orientation="h", y=-0.1),
                      title_font_size=14, title_font_family="Open Sans", font_family="Open Sans")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True, key="chart_status")

# Donut: Backup
with col2:
    bk_counts = df["backup_flag"].value_counts()
    fig2 = px.pie(
        values=bk_counts.values,
        names=bk_counts.index,
        title="Backup Status",
        hole=0.55,
        color_discrete_map={"Ja": "#48bb78", "Nee": "#fc8181"},
    )
    fig2.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                       legend=dict(orientation="h", y=-0.1),
                       title_font_size=14, title_font_family="Open Sans", font_family="Open Sans")
    fig2.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig2, use_container_width=True, key="chart_backup")

# Bar: Tools status
with col3:
    tools_counts = df["tools_status"].value_counts().reset_index()
    tools_counts.columns = ["status", "aantal"]
    tools_counts["label"] = tools_counts["status"].map(TOOLS_LABELS).fillna(tools_counts["status"])
    colors = {
        "OK": "#48bb78", "Verouderd": "#fbd38d",
        "Niet actief": "#fc8181", "Niet geïnstalleerd": "#fc8181", "Onbekend": "#2ea3f2",
    }
    fig3 = px.bar(
        tools_counts, x="label", y="aantal",
        title="VMware Tools Status",
        color="label",
        color_discrete_map=colors,
        text="aantal",
    )
    fig3.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                       showlegend=False, title_font_size=14,
                       xaxis_title="", yaxis_title="Aantal VMs")
    fig3.update_traces(textposition="outside")
    st.plotly_chart(fig3, use_container_width=True, key="chart_tools")


# ─── Aandachtspunten ───────────────────────────────────────────────────────────
if show_alerts:
    no_backup    = df[df["backup_flag"] == "Nee"]
    stale_backup = df[(df["backup_flag"] == "Ja") & (df["dagen_backup"].notna()) & (df["dagen_backup"] > BACKUP_WARN_DAYS)]
    tools_bad    = df[df["tools_status"].isin(["toolsOld", "toolsNotRunning", "toolsNotInstalled"])]
    old_reboot   = df[(df["dagen_reboot"].notna()) & (df["dagen_reboot"] > REBOOT_WARN_DAYS)]
    snaps        = df[df["heeft_snapshot"] == True]

    total_alerts = len(no_backup) + len(stale_backup) + len(tools_bad)

    with st.expander(f"⚠️ Aandachtspunten ({total_alerts} items)", expanded=total_alerts > 0):
        a1, a2, a3 = st.columns(3)

        with a1:
            if not no_backup.empty:
                st.markdown(f'<div class="alert-box"><strong>⛔ {len(no_backup)} VMs zonder backup</strong><br>' +
                            "<br>".join(f"• {n}" for n in no_backup["naam"].tolist()[:10]) +
                            (f"<br>…en {len(no_backup)-10} meer" if len(no_backup) > 10 else "") +
                            "</div>", unsafe_allow_html=True)
            else:
                st.success("✅ Alle VMs hebben backup geconfigureerd")

        with a2:
            if not stale_backup.empty:
                items = [f"• {r['naam']} ({r['backup_datum'].strftime('%d-%m') if r['backup_datum'] else '?'})"
                         for _, r in stale_backup.iterrows()]
                st.markdown(f'<div class="warn-box"><strong>⏰ {len(stale_backup)} VMs – backup ouder dan {BACKUP_WARN_DAYS} dagen</strong><br>' +
                            "<br>".join(items[:10]) + "</div>", unsafe_allow_html=True)
            if not tools_bad.empty:
                label_map2 = {"toolsOld": "Verouderd", "toolsNotRunning": "Niet actief", "toolsNotInstalled": "Niet geïnst."}
                items2 = [f"• {r['naam']} ({label_map2.get(r['tools_status'], '')})" for _, r in tools_bad.iterrows()]
                st.markdown(f'<div class="warn-box"><strong>🔧 {len(tools_bad)} VMs – VMware Tools aandacht</strong><br>' +
                            "<br>".join(items2[:10]) + "</div>", unsafe_allow_html=True)

        with a3:
            if not old_reboot.empty:
                st.markdown(f'<div class="info-box"><strong>🔄 {len(old_reboot)} VMs niet herstart > {REBOOT_WARN_DAYS} dagen</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in old_reboot.head(8).iterrows()) + "</div>",
                            unsafe_allow_html=True)
            if not snaps.empty:
                st.markdown(f'<div class="info-box"><strong>📸 {len(snaps)} VMs met actieve snapshot</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in snaps.iterrows()) + "</div>",
                            unsafe_allow_html=True)


st.divider()


# ─── Tabel ─────────────────────────────────────────────────────────────────────
st.markdown(f"### Servers ({len(df)})")

# Tabel opmaken voor weergave
def fmt_status(s):
    m = {"poweredOn": "✅ Aan", "poweredOff": "❌ Uit", "suspended": "⏸ Gesuspendeerd"}
    return m.get(s, s)

def fmt_tools(s):
    m = {"toolsOk": "✅ OK", "toolsOld": "⚠️ Verouderd", "toolsNotRunning": "🔴 Niet actief",
         "toolsNotInstalled": "🔴 Niet geïnst.", "toolsOnbekend": "– Onbekend"}
    return m.get(s, s)

def fmt_backup(f):
    return "✅ Ja" if f == "Ja" else "❌ Nee"

def fmt_date(dt):
    if dt is None or (hasattr(dt, '__class__') and 'NaT' in str(type(dt))):
        return ""
    try:
        return dt.strftime("%d-%m-%Y %H:%M") if hasattr(dt, 'strftime') else str(dt)
    except Exception:
        return ""

df_display = df[[
    "naam", "status", "locatie", "beheerder", "os",
    "tools_status", "update_moment", "laatste_reboot", "backup_flag", "backup_datum",
    "ip_adres", "vcpu", "geheugen_gib", "sql_versie", "ha_beschermd", "functie"
]].copy()

df_display["status"]       = df_display["status"].map(fmt_status)
df_display["tools_status"] = df_display["tools_status"].map(fmt_tools)
df_display["backup_flag"]  = df_display["backup_flag"].map(fmt_backup)
df_display["backup_datum"] = df["backup_datum"].apply(fmt_date)
df_display["laatste_reboot"] = df["laatste_reboot"].apply(fmt_date)
df_display["ha_beschermd"] = df_display["ha_beschermd"].map({True: "✅ HA", False: "–"})
df_display["geheugen_gib"] = df_display["geheugen_gib"].apply(lambda x: f"{x} GB")
df_display["vcpu_ram"]     = df["vcpu"].astype(str) + " vCPU / " + df["geheugen_gib"].apply(lambda x: f"{x} GB")

col_rename = {
    "naam":           "Naam",
    "status":         "Status",
    "locatie":        "Locatie",
    "beheerder":      "Beheerder",
    "os":             "Besturingssysteem",
    "tools_status":   "VMware Tools",
    "update_moment":  "Patch Schema",
    "laatste_reboot": "Laatste Reboot",
    "backup_flag":    "Backup",
    "backup_datum":   "Laatste Backup",
    "ip_adres":       "IP Adres",
    "vcpu":           "vCPU",
    "geheugen_gib":   "RAM",
    "sql_versie":     "SQL Versie",
    "ha_beschermd":   "HA",
    "functie":        "Functie",
}

df_show = df_display[[
    "naam", "status", "locatie", "beheerder", "os",
    "tools_status", "update_moment", "laatste_reboot",
    "backup_flag", "backup_datum", "ip_adres",
    "vcpu", "geheugen_gib", "sql_versie", "ha_beschermd", "functie"
]].rename(columns=col_rename)

# Interactieve tabel met rij-selectie
event = st.dataframe(
    df_show,
    use_container_width=True,
    height=500,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Naam":              st.column_config.TextColumn("Naam", width="medium"),
        "Status":            st.column_config.TextColumn("Status", width="small"),
        "Locatie":           st.column_config.TextColumn("Locatie", width="medium"),
        "Besturingssysteem": st.column_config.TextColumn("OS", width="large"),
        "VMware Tools":      st.column_config.TextColumn("VMware Tools", width="medium"),
        "IP Adres":          st.column_config.TextColumn("IP Adres", width="small"),
        "vCPU":              st.column_config.NumberColumn("vCPU", width="small"),
        "RAM":               st.column_config.TextColumn("RAM", width="small"),
        "SQL Versie":        st.column_config.TextColumn("SQL", width="small"),
        "HA":                st.column_config.TextColumn("HA", width="small"),
        "Functie":           st.column_config.TextColumn("Functie", width="medium"),
    }
)


# ─── Detailpaneel ──────────────────────────────────────────────────────────────
if show_details:
    selected_rows = event.selection.rows if event and event.selection else []

    if selected_rows:
        idx   = df.index[selected_rows[0]]
        vm    = df.loc[idx]

        st.divider()
        st.markdown(f"## 🔍 Detail: **{vm['naam']}**")

        tab1, tab2, tab3, tab4 = st.tabs(["📋 Overzicht", "💾 Backup & Reboot", "⚙️ VMware Tools", "🌐 Netwerk & Hardware"])

        with tab1:
            d1, d2, d3 = st.columns(3)
            with d1:
                st.markdown("**Algemeen**")
                st.write(f"**Status:** {fmt_status(vm['status'])}")
                st.write(f"**Locatie:** {vm['locatie']}")
                st.write(f"**Datacenter:** {vm['datacenter']}")
                st.write(f"**Cluster:** {vm['cluster']}")
                st.write(f"**ESXi Host:** {vm['esxi_host']}")
                st.write(f"**Aangemaakt:** {vm['aanmaakdatum']}")
            with d2:
                st.markdown("**Beheer**")
                st.write(f"**Beheerder:** {vm['beheerder'] or '–'}")
                st.write(f"**Afdeling:** {vm['afdeling'] or '–'}")
                st.write(f"**Klantnaam:** {vm['klantnaam'] or '–'}")
                st.write(f"**Contract Nr.:** {vm['contract_nr'] or '–'}")
                st.write(f"**Functie:** {vm['functie'] or '–'}")
                st.write(f"**Soort:** {vm['soort'] or '–'}")
            with d3:
                st.markdown("**Hardware**")
                st.write(f"**vCPU's:** {vm['vcpu']}")
                st.write(f"**Geheugen:** {vm['geheugen_gib']} GB")
                st.write(f"**OS:** {vm['os'] or '–'}")
                st.write(f"**Kernel:** {vm['kernel_versie'] or '–'}")
                if vm['sql_versie']:
                    st.write(f"**SQL:** {vm['sql_versie']} ({vm['sql_editie']})")
                st.write(f"**HA Beschermd:** {'✅ Ja' if vm['ha_beschermd'] else '❌ Nee'}")

        with tab2:
            b1, b2 = st.columns(2)
            with b1:
                st.markdown("**Backup**")
                st.write(f"**Geconfigureerd:** {fmt_backup(vm['backup_flag'])}")
                if vm['backup_datum']:
                    dagen = vm['dagen_backup']
                    kleur = "🟢" if dagen <= BACKUP_WARN_DAYS else "🔴"
                    st.write(f"**Laatste backup:** {kleur} {vm['backup_datum'].strftime('%d-%m-%Y %H:%M')} ({dagen} dagen geleden)")
                else:
                    st.write("**Laatste backup:** –")
                if vm['backup_str']:
                    st.markdown("**Veeam detail:**")
                    st.code(vm['backup_str'], language=None)
            with b2:
                st.markdown("**Reboot & Patching**")
                reboot_str = fmt_date(vm['laatste_reboot'])
                if reboot_str:
                    dagen = vm['dagen_reboot'] or 0
                    kleur = "🟢" if dagen <= REBOOT_WARN_DAYS else "🟠"
                    st.write(f"**Laatste reboot (VM):** {kleur} {reboot_str} ({dagen} dagen geleden)")
                else:
                    st.write("**Laatste reboot (VM):** –")
                host_boot_str = fmt_date(vm['host_boot'])
                if host_boot_str:
                    dagen = vm['dagen_host_boot'] or 0
                    kleur = "🟢" if dagen <= REBOOT_WARN_DAYS else "🟠"
                    st.write(f"**Host boot tijd:** {kleur} {host_boot_str} ({dagen} dagen geleden)")
                st.write(f"**ESX Versie:** {vm['esx_versie'] or '–'}")
                st.write(f"**Patch schema:** {vm['update_schema'] or '–'}")
                st.write(f"**Patch moment:** {vm['update_moment'] or '–'}")

        with tab3:
            st.write(f"**Tools Status:** {fmt_tools(vm['tools_status'])}")
            st.write(f"**Tools Versie:** {vm['tools_versie'] or '–'}")
            st.write(f"**Upgradeable:** {vm['tools_upgradeable'] or '–'}")
            st.write(f"**Alarmering:** {vm['alarmering'] or '–'}")
            if vm['heeft_snapshot']:
                st.warning("⚠️ Deze VM heeft een actieve snapshot")

        with tab4:
            st.write(f"**IP Adres:** {vm['ip_adres'] or '–'}")
            if vm['wiki_link']:
                st.markdown(f"**Wiki:** [{vm['wiki_link']}]({vm['wiki_link']})")
            st.write(f"**Cluster:** {vm['cluster'] or '–'}")
            st.write(f"**ESXi Host:** {vm['esxi_host'] or '–'}")

    else:
        st.info("👆 Klik op een rij in de tabel om details te zien.")


# ─── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(f"RAM IT · Prinses Maxima Centrum · Data: vALL-pmc-vCenter.xlsx + Vcenter overzicht PPD-GK · {NOW.strftime('%d-%m-%Y')}")
