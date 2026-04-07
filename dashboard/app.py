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
FILE1         = BASE_DIR / "vALL-pmc-vCenter.xlsx"
FILE2         = BASE_DIR / "Vcenter overzicht Prinses Maxima PPD - GK d.d. 01-04-2026 v1.xlsx"
FILE_MONITOR  = BASE_DIR / "7 x 24 Prinses Maxima Details Windows servers (SCOM vs VMware).xlsx"
FILE_SQL_LIC  = BASE_DIR / "Sql licenties via RAM-IT - Princes Maxima.xlsx"
NOW           = datetime.now()

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
    df_info      = pd.read_excel(FILE1, sheet_name="vInfo",      engine="openpyxl")
    df_tools     = pd.read_excel(FILE1, sheet_name="vTools",     engine="openpyxl")
    df_host      = pd.read_excel(FILE1, sheet_name="vHost",      engine="openpyxl")
    df_snap      = pd.read_excel(FILE1, sheet_name="vSnapshot",  engine="openpyxl")
    df_partition = pd.read_excel(FILE1, sheet_name="vPartition", engine="openpyxl")
    df_memory    = pd.read_excel(FILE1, sheet_name="vMemory",    engine="openpyxl")
    df_cpu       = pd.read_excel(FILE1, sheet_name="vCPU",       engine="openpyxl")
    df_health    = pd.read_excel(FILE1, sheet_name="vHealth",    engine="openpyxl")
    df_network   = pd.read_excel(FILE1, sheet_name="vNetwork",   engine="openpyxl")

    df_info  = df_info[df_info["Template"] != True].copy()

    tools_map = {r["VM"]: r for _, r in df_tools.iterrows()}
    host_map  = {r["Host"]: r for _, r in df_host.iterrows()}
    snap_vms  = set(df_snap[df_snap["Name"] != "VEEAM BACKUP TEMPORARY SNAPSHOT"]["VM"].tolist())

    # ── 24x7 Monitoring (SCOM/Nagios) ────────────────────────────────────────
    monitor_map = {}
    if FILE_MONITOR.exists():
        df_mon = pd.read_excel(FILE_MONITOR, sheet_name="Export", engine="openpyxl")
        for _, r in df_mon.iterrows():
            server = str(r.get("Server", "") or "")
            mon_type = str(r.get(df_mon.columns[3], "") or "") if len(df_mon.columns) > 3 else "SCOM"
            if server:
                short = server.split(".")[0].upper()
                monitor_map[short] = {"type": mon_type, "functie": str(r.get("Functie server", "") or "")}

    # ── SQL licenties ────────────────────────────────────────────────────────
    sql_lic_map = {}
    if FILE_SQL_LIC.exists():
        df_sql = pd.read_excel(FILE_SQL_LIC, sheet_name="Export", engine="openpyxl")
        for _, r in df_sql.iterrows():
            contract = str(r.get("Contract", "") or "")
            vm = str(r.get("Virtual Machine", "") or "")
            if vm and contract not in ("Total", "None", "") and vm != "None":
                sql_lic_map[vm.upper()] = {
                    "edition": str(r.get("SQLEdition", "") or ""),
                    "version": str(r.get("SQLVersion", "") or ""),
                }

    # Partitie-aggregatie per VM
    part_map = {}
    for _, p in df_partition.iterrows():
        vm = p["VM"]
        entry = {"disk": str(p.get("Disk", "")), "capacity_mib": p.get("Capacity MiB", 0),
                 "consumed_mib": p.get("Consumed MiB", 0), "free_mib": p.get("Free MiB", 0),
                 "free_pct": int(p.get("Free %", 0) or 0)}
        part_map.setdefault(vm, []).append(entry)

    # Memory per VM
    mem_map = {}
    for _, m in df_memory.iterrows():
        mem_map[m["VM"]] = {
            "mem_size": int(m.get("Size MiB", 0) or 0),
            "mem_consumed": int(m.get("Consumed", 0) or 0),
            "mem_active": int(m.get("Active", 0) or 0),
            "mem_swapped": int(m.get("Swapped", 0) or 0),
            "mem_ballooned": int(m.get("Ballooned", 0) or 0),
        }

    # CPU per VM
    cpu_map = {}
    for _, c in df_cpu.iterrows():
        max_mhz = int(c.get("Max", 0) or 0)
        overall  = int(c.get("Overall", 0) or 0)
        cpu_map[c["VM"]] = {
            "cpu_overall_mhz": overall,
            "cpu_max_mhz": max_mhz,
            "cpu_pct": round(overall / max_mhz * 100) if max_mhz else 0,
        }

    # Health messages per VM
    health_map = {}
    for _, h in df_health.iterrows():
        vm = h.get("Name", "")
        msg = str(h.get("Message", "") or "")
        if vm and msg:
            health_map.setdefault(vm, []).append(msg)

    # Network per VM
    nic_map = {}
    for _, n in df_network.iterrows():
        vm = n["VM"]
        nic_map.setdefault(vm, []).append({
            "nic": str(n.get("NIC label", "") or ""),
            "network": str(n.get("Network", "") or ""),
            "switch": str(n.get("Switch", "") or ""),
            "connected": bool(n.get("Connected", False)),
            "mac": str(n.get("Mac Address", "") or ""),
            "ipv4": str(n.get("IPv4 Address", "") or ""),
        })

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
            # Nieuwe velden
            "partitions":        part_map.get(vm, []),
            "min_free_pct":      min((p["free_pct"] for p in part_map.get(vm, [])), default=None),
            "mem_size":          mem_map.get(vm, {}).get("mem_size"),
            "mem_consumed":      mem_map.get(vm, {}).get("mem_consumed"),
            "mem_active":        mem_map.get(vm, {}).get("mem_active"),
            "mem_swapped":       mem_map.get(vm, {}).get("mem_swapped"),
            "mem_ballooned":     mem_map.get(vm, {}).get("mem_ballooned"),
            "mem_pct":           round(mem_map.get(vm, {}).get("mem_consumed", 0) / mem_map.get(vm, {}).get("mem_size", 1) * 100) if mem_map.get(vm, {}).get("mem_size") else None,
            "cpu_overall_mhz":   cpu_map.get(vm, {}).get("cpu_overall_mhz"),
            "cpu_max_mhz":       cpu_map.get(vm, {}).get("cpu_max_mhz"),
            "cpu_pct":           cpu_map.get(vm, {}).get("cpu_pct"),
            "health_messages":   health_map.get(vm, []),
            "nics":              nic_map.get(vm, []),
            "monitoring_type":   monitor_map.get(vm.upper(), {}).get("type", ""),
            "monitoring_functie": monitor_map.get(vm.upper(), {}).get("functie", ""),
            "sql_lic_edition":   sql_lic_map.get(vm.upper(), {}).get("edition", ""),
            "sql_lic_version":   sql_lic_map.get(vm.upper(), {}).get("version", ""),
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
            # Nieuwe velden (niet beschikbaar in bestand 2)
            "partitions":        [],
            "min_free_pct":      None,
            "mem_size":          None,
            "mem_consumed":      None,
            "mem_active":        None,
            "mem_swapped":       None,
            "mem_ballooned":     None,
            "mem_pct":           None,
            "cpu_overall_mhz":   None,
            "cpu_max_mhz":       None,
            "cpu_pct":           None,
            "health_messages":   [],
            "nics":              [],
            "monitoring_type":   monitor_map.get(str(r.get("VM", "")).upper(), {}).get("type", ""),
            "monitoring_functie": monitor_map.get(str(r.get("VM", "")).upper(), {}).get("functie", ""),
            "sql_lic_edition":   sql_lic_map.get(str(r.get("VM", "")).upper(), {}).get("edition", ""),
            "sql_lic_version":   sql_lic_map.get(str(r.get("VM", "")).upper(), {}).get("version", ""),
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
  @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;700;800&display=swap');

  /* Base font */
  html, body, [class*="css"] { font-family: 'Open Sans', -apple-system, sans-serif !important; }

  /* KPI metrics */
  [data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 800 !important; color: #5b2882 !important; }
  [data-testid="stMetricLabel"] { font-size: 0.7rem !important; text-transform: uppercase; letter-spacing: 0.6px; color: #666 !important; }
  [data-testid="stMetricDelta"] { color: #c49a2c !important; }

  /* Sidebar: gradient matching RAM presentation */
  [data-testid="stSidebar"] {
    background: linear-gradient(180deg, #5b2882 0%, #3d5a9e 60%, #4a8fd9 100%) !important;
  }
  [data-testid="stSidebar"] * { color: white !important; }
  [data-testid="stSidebar"] label { color: rgba(255,255,255,0.7) !important; font-size: 11px !important; text-transform: uppercase; letter-spacing: 0.5px; }
  [data-testid="stSidebar"] .stSelectbox > div > div { background: rgba(255,255,255,0.12) !important; border: 1px solid rgba(255,255,255,0.2) !important; color: white !important; }
  [data-testid="stSidebar"] input { background: rgba(255,255,255,0.12) !important; border: 1px solid rgba(255,255,255,0.2) !important; color: white !important; }
  [data-testid="stSidebar"] .stCheckbox label span { color: rgba(255,255,255,0.9) !important; }
  [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.15) !important; }

  /* Status badges */
  .status-aan   { color: #276749; background: #c6f6d5; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
  .status-uit   { color: #9b2c2c; background: #fed7d7; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
  .status-susp  { color: #744210; background: #fefcbf; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 700; }

  /* Alert boxes */
  .alert-box    { border-left: 4px solid #e53e3e; background: #fff5f5; padding: 12px 16px; border-radius: 0 10px 10px 0; margin-bottom: 8px; }
  .warn-box     { border-left: 4px solid #c49a2c; background: #fefcf0; padding: 12px 16px; border-radius: 0 10px 10px 0; margin-bottom: 8px; }
  .info-box     { border-left: 4px solid #4a8fd9; background: #f0f6ff; padding: 12px 16px; border-radius: 0 10px 10px 0; margin-bottom: 8px; }

  /* Expander */
  div[data-testid="stExpander"] summary { font-weight: 600; }
  div[data-testid="stExpander"] { border: 1px solid #e8e0f0 !important; border-radius: 10px !important; }

  /* Buttons */
  .stButton > button { background: linear-gradient(135deg, #5b2882, #4a8fd9) !important; color: white !important; border: none !important; font-family: 'Open Sans', sans-serif !important; font-weight: 600 !important; border-radius: 8px !important; }
  .stButton > button:hover { background: linear-gradient(135deg, #4a2070, #3d7bc4) !important; }

  /* Tabs styling */
  .stTabs [data-baseweb="tab-list"] { gap: 4px; }
  .stTabs [data-baseweb="tab"] { background: #f5f0fa; border-radius: 8px 8px 0 0; padding: 8px 16px; }
  .stTabs [aria-selected="true"] { background: #5b2882 !important; color: white !important; }

  /* Dividers: gold accent */
  hr { border-color: #e8dcc8 !important; }

  /* Dataframe header */
  [data-testid="stDataFrame"] th { background: #5b2882 !important; color: white !important; }
</style>
""", unsafe_allow_html=True)


# ─── Data laden ────────────────────────────────────────────────────────────────
df_all = load_data()


# ─── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 12px 0;margin-bottom:4px">
      <div style="color:white;font-family:'Open Sans',sans-serif;font-size:24px;letter-spacing:-0.5px">
        <span style="font-weight:300">ram</span> <span style="font-weight:800">infotechnology</span>
      </div>
      <div style="width:40px;height:3px;background:#c49a2c;border-radius:2px;margin-top:8px"></div>
      <div style="color:rgba(255,255,255,0.7);font-size:11px;margin-top:8px;text-transform:uppercase;letter-spacing:1px">Server Overzicht</div>
      <div style="color:rgba(255,255,255,0.5);font-size:11px;margin-top:2px">Prinses Maxima Centrum</div>
    </div>
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
<div style="background:linear-gradient(135deg,#5b2882 0%,#3d5a9e 50%,#4a8fd9 100%);padding:24px 32px;border-radius:14px;margin-bottom:24px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 4px 20px rgba(91,40,130,0.25)">
  <div>
    <div style="color:rgba(255,255,255,0.6);font-size:10px;text-transform:uppercase;letter-spacing:2px;margin-bottom:4px">VMware vCenter Rapportage</div>
    <div style="color:white;font-family:'Open Sans',sans-serif;font-size:24px;font-weight:800;letter-spacing:-0.3px">Server Overzicht</div>
    <div style="color:rgba(255,255,255,0.8);font-size:13px;margin-top:2px">Prinses Maxima Centrum</div>
    <div style="width:50px;height:3px;background:#c49a2c;border-radius:2px;margin-top:10px"></div>
    <div style="color:rgba(255,255,255,0.6);font-size:11px;margin-top:8px">{len(df)} van {len(df_all)} servers zichtbaar</div>
  </div>
  <div style="text-align:right">
    <div style="color:white;font-family:'Open Sans',sans-serif;font-size:20px;letter-spacing:-0.5px"><span style="font-weight:300">ram</span> <span style="font-weight:800">infotechnology</span></div>
    <div style="color:rgba(255,255,255,0.5);font-size:11px;margin-top:4px">Gegenereerd {NOW.strftime('%d-%m-%Y %H:%M')}</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ─── KPI Cards ────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)
total    = len(df)
aan      = (df["status"] == "poweredOn").sum()
uit      = (df["status"] == "poweredOff").sum()
bk_ja    = (df["backup_flag"] == "Ja").sum()
bk_pct   = round(bk_ja / total * 100) if total else 0
tools_is = ((df["tools_status"] != "toolsOk") & (df["tools_status"] != "toolsOnbekend")).sum()
mon_count = (df["monitoring_type"] != "").sum()

k1.metric("Totaal servers",     total)
k2.metric("Aan",                aan,   delta=f"{round(aan/total*100)}%" if total else None)
k3.metric("Uit",                uit,   delta=f"-{uit}" if uit else None, delta_color="inverse")
k4.metric("Backup",             f"{bk_pct}%", delta=f"{bk_ja}/{total}")
k5.metric("24x7 monitoring",    mon_count, delta=f"{mon_count}/{total}")
k6.metric("Tools aandacht",     tools_is, delta="OK" if tools_is == 0 else f"{tools_is} VMs", delta_color="inverse" if tools_is > 0 else "normal")


st.divider()


# ─── Grafieken ─────────────────────────────────────────────────────────────────
row1_c1, row1_c2, row1_c3 = st.columns(3)

# Donut: Status
with row1_c1:
    status_counts = df["status"].value_counts()
    label_map = {"poweredOn": "Aan", "poweredOff": "Uit", "suspended": "Gesuspendeerd"}
    fig = px.pie(
        values=status_counts.values,
        names=[label_map.get(s, s) for s in status_counts.index],
        title="Server Status",
        hole=0.55,
        color_discrete_sequence=["#48bb78", "#e05a6b", "#c49a2c"],
    )
    fig.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                      legend=dict(orientation="h", y=-0.1),
                      title_font_size=14, title_font_family="Open Sans", font_family="Open Sans")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True, key="chart_status")

# Donut: Backup
with row1_c2:
    bk_counts = df["backup_flag"].value_counts()
    fig2 = px.pie(
        values=bk_counts.values,
        names=bk_counts.index,
        title="Backup Status",
        hole=0.55,
        color_discrete_map={"Ja": "#48bb78", "Nee": "#e05a6b"},
    )
    fig2.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                       legend=dict(orientation="h", y=-0.1),
                       title_font_size=14, title_font_family="Open Sans", font_family="Open Sans")
    fig2.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig2, use_container_width=True, key="chart_backup")

# Bar: Tools status
with row1_c3:
    tools_counts = df["tools_status"].value_counts().reset_index()
    tools_counts.columns = ["status", "aantal"]
    tools_counts["label"] = tools_counts["status"].map(TOOLS_LABELS).fillna(tools_counts["status"])
    colors = {
        "OK": "#48bb78", "Verouderd": "#c49a2c",
        "Niet actief": "#e05a6b", "Niet geïnstalleerd": "#e05a6b", "Onbekend": "#4a8fd9",
    }
    fig3 = px.bar(
        tools_counts, x="label", y="aantal",
        title="VMware Tools Status",
        color="label",
        color_discrete_map=colors,
        text="aantal",
    )
    fig3.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=260,
                       showlegend=False, title_font_size=14, title_font_family="Open Sans", font_family="Open Sans",
                       xaxis_title="", yaxis_title="Aantal VMs")
    fig3.update_traces(textposition="outside")
    st.plotly_chart(fig3, use_container_width=True, key="chart_tools")

# Rij 2: Schijfruimte + CPU/RAM
row2_c1, row2_c2 = st.columns(2)

with row2_c1:
    disk_df = df[df["min_free_pct"].notna()].copy()
    disk_df["min_free_pct"] = pd.to_numeric(disk_df["min_free_pct"], errors="coerce")
    disk_data = disk_df.nsmallest(10, "min_free_pct")[["naam", "min_free_pct"]].copy()
    if not disk_data.empty:
        disk_data["min_free_pct"] = disk_data["min_free_pct"].astype(int)
        disk_data["kleur"] = disk_data["min_free_pct"].apply(lambda x: "#fc8181" if x < 15 else ("#fbd38d" if x < 30 else "#48bb78"))
        fig4 = px.bar(disk_data, y="naam", x="min_free_pct", orientation="h",
                       title="Top 10 laagste schijfruimte (% vrij)", text="min_free_pct",
                       color="min_free_pct", color_continuous_scale=["#e05a6b", "#c49a2c", "#48bb78"],
                       range_color=[0, 100])
        fig4.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=300,
                           showlegend=False, title_font_size=14, title_font_family="Open Sans", font_family="Open Sans",
                           xaxis_title="% vrij", yaxis_title="", coloraxis_showscale=False,
                           yaxis=dict(autorange="reversed"))
        fig4.update_traces(texttemplate="%{text}%", textposition="outside")
        st.plotly_chart(fig4, use_container_width=True, key="chart_disk")
    else:
        st.info("Geen schijfdata beschikbaar")

with row2_c2:
    perf_data = df[(df["cpu_pct"].notna()) & (df["mem_pct"].notna())][["naam", "cpu_pct", "mem_pct", "geheugen_gib"]].copy()
    if not perf_data.empty:
        fig5 = px.scatter(perf_data, x="cpu_pct", y="mem_pct", size="geheugen_gib",
                          hover_name="naam", title="CPU vs RAM gebruik per VM",
                          labels={"cpu_pct": "CPU %", "mem_pct": "RAM %", "geheugen_gib": "Geheugen (GB)"},
                          color_discrete_sequence=["#5b2882"])
        fig5.add_hline(y=95, line_dash="dash", line_color="#e05a6b", annotation_text="RAM 95%")
        fig5.add_vline(x=90, line_dash="dash", line_color="#e05a6b", annotation_text="CPU 90%")
        fig5.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=300,
                           title_font_size=14, title_font_family="Open Sans", font_family="Open Sans")
        st.plotly_chart(fig5, use_container_width=True, key="chart_perf")
    else:
        st.info("Geen CPU/RAM data beschikbaar")


# ─── Aandachtspunten ───────────────────────────────────────────────────────────
if show_alerts:
    no_backup    = df[df["backup_flag"] == "Nee"]
    stale_backup = df[(df["backup_flag"] == "Ja") & (df["dagen_backup"].notna()) & (df["dagen_backup"] > BACKUP_WARN_DAYS)]
    tools_bad    = df[df["tools_status"].isin(["toolsOld", "toolsNotRunning", "toolsNotInstalled"])]
    old_reboot   = df[(df["dagen_reboot"].notna()) & (df["dagen_reboot"] > REBOOT_WARN_DAYS)]
    snaps        = df[df["heeft_snapshot"] == True]
    disk_low     = df[(df["min_free_pct"].notna()) & (df["min_free_pct"] < 15)]
    mem_swap     = df[(df["mem_swapped"].notna()) & ((df["mem_swapped"] > 0) | (df["mem_ballooned"] > 0))]
    cpu_high     = df[(df["cpu_pct"].notna()) & (df["cpu_pct"] > 90)]
    health_warns = df[df["health_messages"].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)]
    no_monitor   = df[(df["monitoring_type"] == "") & (df["status"] == "poweredOn")]

    total_alerts = len(no_backup) + len(stale_backup) + len(tools_bad) + len(disk_low) + len(mem_swap) + len(cpu_high) + len(no_monitor)

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
            if not disk_low.empty:
                items_d = [f"• {r['naam']} ({int(r['min_free_pct'])}% vrij)" for _, r in disk_low.iterrows()]
                st.markdown(f'<div class="alert-box"><strong>💾 {len(disk_low)} VMs schijfruimte &lt; 15% vrij</strong><br>' +
                            "<br>".join(items_d[:10]) + "</div>", unsafe_allow_html=True)

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
            if not mem_swap.empty:
                items_m = [f"• {r['naam']} (swap: {r['mem_swapped']}M / balloon: {r['mem_ballooned']}M)" for _, r in mem_swap.iterrows()]
                st.markdown(f'<div class="warn-box"><strong>🧠 {len(mem_swap)} VMs met geheugen swap/balloon</strong><br>' +
                            "<br>".join(items_m[:10]) + "</div>", unsafe_allow_html=True)

        with a3:
            if not cpu_high.empty:
                items_c = [f"• {r['naam']} ({int(r['cpu_pct'])}%)" for _, r in cpu_high.iterrows()]
                st.markdown(f'<div class="alert-box"><strong>🔥 {len(cpu_high)} VMs CPU-gebruik &gt; 90%</strong><br>' +
                            "<br>".join(items_c[:10]) + "</div>", unsafe_allow_html=True)
            if not old_reboot.empty:
                st.markdown(f'<div class="info-box"><strong>🔄 {len(old_reboot)} VMs niet herstart > {REBOOT_WARN_DAYS} dagen</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in old_reboot.head(8).iterrows()) + "</div>",
                            unsafe_allow_html=True)
            if not snaps.empty:
                st.markdown(f'<div class="info-box"><strong>📸 {len(snaps)} VMs met actieve snapshot</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in snaps.iterrows()) + "</div>",
                            unsafe_allow_html=True)
            if not health_warns.empty:
                st.markdown(f'<div class="info-box"><strong>⚕️ {len(health_warns)} VMs met health waarschuwingen</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in health_warns.head(8).iterrows()) + "</div>",
                            unsafe_allow_html=True)
            if not no_monitor.empty:
                st.markdown(f'<div class="warn-box"><strong>📡 {len(no_monitor)} actieve VMs zonder 24x7 monitoring</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in no_monitor.head(10).iterrows()) +
                            (f"<br>…en {len(no_monitor)-10} meer" if len(no_monitor) > 10 else "") + "</div>",
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

def fmt_monitoring(t):
    if not t or t == "":
        return "–"
    if t == "SCOM":
        return "🟣 SCOM"
    if t == "Nagios":
        return "🔵 Nagios"
    return t

def fmt_pct(val, thresholds_green, thresholds_orange):
    """Badge voor percentages. thresholds in 'lager is slechter' modus (schijf) of 'hoger is slechter' modus (CPU/RAM)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    val = int(val)
    return f"{val}%"

def fmt_disk_pct(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    val = int(val)
    if val < 15:
        return f"🔴 {val}%"
    elif val < 30:
        return f"🟠 {val}%"
    return f"🟢 {val}%"

def fmt_cpu_pct(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    val = int(val)
    if val > 90:
        return f"🔴 {val}%"
    elif val > 70:
        return f"🟠 {val}%"
    return f"🟢 {val}%"

def fmt_ram_pct(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    val = int(val)
    if val > 95:
        return f"🔴 {val}%"
    elif val > 80:
        return f"🟠 {val}%"
    return f"🟢 {val}%"

def fmt_date(dt):
    if dt is None or (hasattr(dt, '__class__') and 'NaT' in str(type(dt))):
        return ""
    try:
        return dt.strftime("%d-%m-%Y %H:%M") if hasattr(dt, 'strftime') else str(dt)
    except Exception:
        return ""

df_display = df[[
    "naam", "status", "locatie", "beheerder", "os",
    "tools_status", "monitoring_type", "update_moment", "laatste_reboot", "backup_flag", "backup_datum",
    "ip_adres", "vcpu", "geheugen_gib", "min_free_pct", "cpu_pct", "mem_pct",
    "sql_versie", "ha_beschermd", "functie"
]].copy()

df_display["status"]           = df_display["status"].map(fmt_status)
df_display["tools_status"]     = df_display["tools_status"].map(fmt_tools)
df_display["monitoring_type"]  = df_display["monitoring_type"].apply(fmt_monitoring)
df_display["backup_flag"]      = df_display["backup_flag"].map(fmt_backup)
df_display["backup_datum"]     = df["backup_datum"].apply(fmt_date)
df_display["laatste_reboot"]   = df["laatste_reboot"].apply(fmt_date)
df_display["ha_beschermd"]     = df_display["ha_beschermd"].map({True: "✅ HA", False: "–"})
df_display["geheugen_gib"]     = df_display["geheugen_gib"].apply(lambda x: f"{x} GB")
df_display["min_free_pct"]     = df["min_free_pct"].apply(fmt_disk_pct)
df_display["cpu_pct"]          = df["cpu_pct"].apply(fmt_cpu_pct)
df_display["mem_pct"]          = df["mem_pct"].apply(fmt_ram_pct)
df_display["vcpu_ram"]         = df.apply(lambda r: f"{r['vcpu']} vCPU / {r['geheugen_gib']} GB", axis=1) if not df.empty else pd.Series([], dtype=str)

col_rename = {
    "naam":             "Naam",
    "status":           "Status",
    "locatie":          "Locatie",
    "beheerder":        "Beheerder",
    "os":               "Besturingssysteem",
    "tools_status":     "VMware Tools",
    "monitoring_type":  "24x7",
    "update_moment":    "Patch Schema",
    "laatste_reboot":   "Laatste Reboot",
    "backup_flag":      "Backup",
    "backup_datum":     "Laatste Backup",
    "ip_adres":         "IP Adres",
    "vcpu":             "vCPU",
    "geheugen_gib":     "RAM",
    "min_free_pct":     "Schijf Vrij",
    "cpu_pct":          "CPU",
    "mem_pct":          "RAM %",
    "sql_versie":       "SQL Versie",
    "ha_beschermd":     "HA",
    "functie":          "Functie",
}

df_show = df_display[[
    "naam", "status", "locatie", "beheerder", "os",
    "tools_status", "monitoring_type", "min_free_pct", "cpu_pct", "mem_pct",
    "update_moment", "laatste_reboot",
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
        "24x7":              st.column_config.TextColumn("24x7", width="small"),
        "Schijf Vrij":       st.column_config.TextColumn("Schijf", width="small"),
        "CPU":               st.column_config.TextColumn("CPU", width="small"),
        "RAM %":             st.column_config.TextColumn("RAM %", width="small"),
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

        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Overzicht", "💾 Opslag", "🔄 Backup & Reboot", "⚙️ VMware Tools & Health", "🌐 Netwerk"])

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
                st.markdown("**Hardware & Performance**")
                st.write(f"**vCPU's:** {vm['vcpu']}")
                if vm['cpu_pct'] is not None and not (isinstance(vm['cpu_pct'], float) and pd.isna(vm['cpu_pct'])):
                    st.write(f"**CPU gebruik:** {fmt_cpu_pct(vm['cpu_pct'])} ({vm['cpu_overall_mhz']} / {vm['cpu_max_mhz']} MHz)")
                else:
                    st.write("**CPU gebruik:** –")
                st.write(f"**Geheugen:** {vm['geheugen_gib']} GB")
                if vm['mem_pct'] is not None and not (isinstance(vm['mem_pct'], float) and pd.isna(vm['mem_pct'])):
                    st.write(f"**RAM gebruik:** {fmt_ram_pct(vm['mem_pct'])} ({vm['mem_consumed']} / {vm['mem_size']} MiB)")
                    if vm['mem_swapped'] and vm['mem_swapped'] > 0:
                        st.write(f"**⚠️ Swapped:** {vm['mem_swapped']} MiB")
                    if vm['mem_ballooned'] and vm['mem_ballooned'] > 0:
                        st.write(f"**⚠️ Ballooned:** {vm['mem_ballooned']} MiB")
                else:
                    st.write("**RAM gebruik:** –")
                st.write(f"**OS:** {vm['os'] or '–'}")
                st.write(f"**Kernel:** {vm['kernel_versie'] or '–'}")
                if vm['sql_lic_edition']:
                    st.write(f"**SQL Licentie:** {vm['sql_lic_version']} ({vm['sql_lic_edition']})")
                elif vm['sql_versie']:
                    st.write(f"**SQL:** {vm['sql_versie']} ({vm['sql_editie']})")
                st.write(f"**HA Beschermd:** {'✅ Ja' if vm['ha_beschermd'] else '❌ Nee'}")

        with tab2:
            partitions = vm['partitions'] if isinstance(vm['partitions'], list) else []
            if partitions:
                st.markdown("**Schijfpartities**")
                for p in partitions:
                    cap   = p['capacity_mib']
                    used  = p['consumed_mib']
                    free  = p['free_pct']
                    pct   = round((1 - free / 100) * 100) if cap else 0
                    color = "#48bb78" if free >= 30 else ("#c49a2c" if free >= 15 else "#e05a6b")
                    st.markdown(f"""
                    <div style="margin-bottom:10px">
                      <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:3px">
                        <span>{p['disk'] or 'Onbekend'}</span>
                        <span>{free}% vrij ({round(p['free_mib']/1024, 1)} GB van {round(cap/1024, 1)} GB)</span>
                      </div>
                      <div style="background:#e2e8f0;border-radius:6px;height:14px;overflow:hidden">
                        <div style="background:{color};height:100%;width:{pct}%;border-radius:6px;transition:width 0.3s"></div>
                      </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("Geen partitie-data beschikbaar voor deze VM.")

        with tab3:
            b1, b2 = st.columns(2)
            with b1:
                st.markdown("**Backup**")
                st.write(f"**Geconfigureerd:** {fmt_backup(vm['backup_flag'])}")
                backup_dt_str = fmt_date(vm['backup_datum'])
                if backup_dt_str:
                    dagen = vm['dagen_backup']
                    kleur = "🟢" if dagen <= BACKUP_WARN_DAYS else "🔴"
                    st.write(f"**Laatste backup:** {kleur} {backup_dt_str} ({dagen} dagen geleden)")
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

        with tab4:
            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**VMware Tools**")
                st.write(f"**Tools Status:** {fmt_tools(vm['tools_status'])}")
                st.write(f"**Tools Versie:** {vm['tools_versie'] or '–'}")
                st.write(f"**Upgradeable:** {vm['tools_upgradeable'] or '–'}")
                st.divider()
                st.markdown("**24x7 Monitoring**")
                if vm['monitoring_type']:
                    st.write(f"**Type:** {fmt_monitoring(vm['monitoring_type'])}")
                    if vm['monitoring_functie']:
                        st.write(f"**Functie (monitoring):** {vm['monitoring_functie']}")
                else:
                    st.write("**Type:** – Geen 24x7 monitoring")
                if vm['alarmering']:
                    st.write(f"**Alarmering (vCenter):** {vm['alarmering']}")
                if vm['heeft_snapshot']:
                    st.warning("⚠️ Deze VM heeft een actieve snapshot")
            with t2:
                health = vm['health_messages'] if isinstance(vm['health_messages'], list) else []
                if health:
                    st.markdown("**Health Waarschuwingen**")
                    for msg in health:
                        st.warning(msg)
                else:
                    st.success("✅ Geen health waarschuwingen")

        with tab5:
            nics = vm['nics'] if isinstance(vm['nics'], list) else []
            if nics:
                st.markdown("**Netwerk Adapters**")
                nic_df = pd.DataFrame(nics)
                nic_df.columns = ["NIC", "VLAN", "Switch", "Connected", "MAC", "IPv4"]
                nic_df["Connected"] = nic_df["Connected"].map({True: "✅", False: "❌"})
                st.dataframe(nic_df, use_container_width=True, hide_index=True)
            else:
                st.info("Geen netwerkdata beschikbaar voor deze VM.")
            st.write(f"**IP Adres (primair):** {vm['ip_adres'] or '–'}")
            if vm['wiki_link']:
                st.markdown(f"**Wiki:** [{vm['wiki_link']}]({vm['wiki_link']})")

    else:
        st.info("👆 Klik op een rij in de tabel om details te zien.")


# ─── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(f"RAM IT · Prinses Maxima Centrum · Data: vALL-pmc-vCenter.xlsx + Vcenter overzicht PPD-GK · {NOW.strftime('%d-%m-%Y')}")
