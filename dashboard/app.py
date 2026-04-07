"""
Prinses Maxima Server Dashboard – Streamlit App
Starten: streamlit run app.py
"""

import io
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

def fmt_date(dt):
    if dt is None or (hasattr(dt, '__class__') and 'NaT' in str(type(dt))):
        return ""
    try:
        return dt.strftime("%d-%m-%Y %H:%M") if hasattr(dt, 'strftime') else str(dt)
    except Exception:
        return ""


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

    # Volledig scherm gradient achtergrond + verberg sidebar
    st.markdown("""
    <style>
      [data-testid="stSidebar"] { display: none; }
      .stApp { background: linear-gradient(135deg, #5b2882 0%, #3d5a9e 50%, #4a8fd9 100%) !important; }
      [data-testid="stMainBlockContainer"] { display: flex; align-items: center; justify-content: center; min-height: 80vh; }
    </style>
    """, unsafe_allow_html=True)

    # Centreer login card
    spacer_l, card, spacer_r = st.columns([1, 1.2, 1])
    with card:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:40px 36px 32px 36px;box-shadow:0 8px 32px rgba(0,0,0,0.18);text-align:center">
          <div style="font-family:'Open Sans',sans-serif;font-size:24px;letter-spacing:-0.5px;margin-bottom:4px">
            <span style="font-weight:300;color:#5b2882">ram</span> <span style="font-weight:800;color:#5b2882">infotechnology</span>
          </div>
          <div style="width:36px;height:3px;background:#c49a2c;border-radius:2px;margin:10px auto 20px auto"></div>
          <div style="color:#888;font-size:13px;margin-bottom:4px">Server Overzicht</div>
          <div style="color:#aaa;font-size:12px;margin-bottom:0px">Prinses Maxima Centrum</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        pw = st.text_input("Wachtwoord", type="password", key="pw_input", label_visibility="collapsed", placeholder="Wachtwoord")
        if st.button("Inloggen", use_container_width=True):
            if pw == st.secrets.get("APP_PASSWORD", ""):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Onjuist wachtwoord.")
    return False

if not check_password():
    st.stop()

# ─── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;700;800&display=swap');

  html, body, [class*="css"] { font-family: 'Open Sans', -apple-system, sans-serif !important; }

  /* KPI metrics */
  [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 800 !important; color: #5b2882 !important; }
  [data-testid="stMetricLabel"] { font-size: 0.65rem !important; text-transform: uppercase; letter-spacing: 0.6px; color: #888 !important; }
  [data-testid="stMetricDelta"] { font-size: 0.75rem !important; }

  /* Sidebar */
  [data-testid="stSidebar"] { background: linear-gradient(180deg, #5b2882 0%, #3d5a9e 60%, #4a8fd9 100%) !important; }
  [data-testid="stSidebar"] * { color: white !important; }
  [data-testid="stSidebar"] label { color: rgba(255,255,255,0.6) !important; font-size: 10px !important; text-transform: uppercase; letter-spacing: 0.5px; }
  [data-testid="stSidebar"] .stSelectbox > div > div { background: rgba(255,255,255,0.1) !important; border: 1px solid rgba(255,255,255,0.15) !important; }
  [data-testid="stSidebar"] input { background: rgba(255,255,255,0.1) !important; border: 1px solid rgba(255,255,255,0.15) !important; }
  [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.1) !important; }

  /* Alert boxes */
  .alert-box    { border-left: 4px solid #e05a6b; background: #fef2f2; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 6px; font-size: 13px; }
  .warn-box     { border-left: 4px solid #c49a2c; background: #fefcf0; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 6px; font-size: 13px; }
  .info-box     { border-left: 4px solid #4a8fd9; background: #f0f6ff; padding: 10px 14px; border-radius: 0 8px 8px 0; margin-bottom: 6px; font-size: 13px; }

  /* Expanders */
  div[data-testid="stExpander"] summary { font-weight: 600; }
  div[data-testid="stExpander"] { border: 1px solid #ede8f3 !important; border-radius: 10px !important; }

  /* Buttons */
  .stButton > button { background: linear-gradient(135deg, #5b2882, #4a8fd9) !important; color: white !important; border: none !important; font-weight: 600 !important; border-radius: 8px !important; font-size: 13px !important; }
  .stButton > button:hover { background: linear-gradient(135deg, #4a2070, #3d7bc4) !important; }
  .stDownloadButton > button { background: white !important; color: #5b2882 !important; border: 2px solid #5b2882 !important; font-weight: 600 !important; border-radius: 8px !important; font-size: 12px !important; }
  .stDownloadButton > button:hover { background: #f5f0fa !important; }

  /* Tabs */
  .stTabs [data-baseweb="tab"] { background: #f8f5fb; border-radius: 8px 8px 0 0; }
  .stTabs [aria-selected="true"] { background: #5b2882 !important; color: white !important; }

  /* Dividers */
  hr { border-color: #ede8f3 !important; }
</style>
""", unsafe_allow_html=True)


# ─── Data laden ────────────────────────────────────────────────────────────────
df_all = load_data()


# ─── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:4px 0 16px 0">
      <div style="color:white;font-family:'Open Sans',sans-serif;font-size:22px;letter-spacing:-0.5px">
        <span style="font-weight:300">ram</span> <span style="font-weight:800">infotechnology</span>
      </div>
      <div style="width:36px;height:2px;background:#c49a2c;border-radius:2px;margin-top:8px"></div>
    </div>
    """, unsafe_allow_html=True)
    st.caption(f"Prinses Maxima Centrum · {NOW.strftime('%d-%m-%Y')}")

    if st.button("🔄 Ververs data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    locaties   = ["Alle"] + sorted(df_all["locatie"].dropna().unique().tolist())
    beheerders = ["Alle"] + sorted([b for b in df_all["beheerder"].dropna().unique().tolist() if b])
    soorten    = ["Alle"] + sorted([s for s in df_all["soort"].dropna().unique().tolist() if s])

    f_search    = st.text_input("🔍 Zoeken", placeholder="Servernaam, IP, OS…")
    f_locatie   = st.selectbox("Locatie",      locaties)
    f_status    = st.selectbox("Status",       ["Alle", "Aan", "Uit"])
    f_beheerder = st.selectbox("Beheerder",    beheerders)
    f_backup    = st.selectbox("Backup",       ["Alle", "Ja", "Nee"])
    f_monitoring = st.selectbox("Monitoring",  ["Alle", "SCOM", "Nagios", "Geen"])


# ─── Filter toepassen ──────────────────────────────────────────────────────────
df = df_all.copy()
if f_locatie != "Alle":
    df = df[df["locatie"] == f_locatie]
if f_status == "Aan":
    df = df[df["status"] == "poweredOn"]
elif f_status == "Uit":
    df = df[df["status"] == "poweredOff"]
if f_beheerder != "Alle":
    df = df[df["beheerder"] == f_beheerder]
if f_backup == "Ja":
    df = df[df["backup_flag"] == "Ja"]
elif f_backup == "Nee":
    df = df[df["backup_flag"] == "Nee"]
if f_monitoring == "SCOM":
    df = df[df["monitoring_type"] == "SCOM"]
elif f_monitoring == "Nagios":
    df = df[df["monitoring_type"] == "Nagios"]
elif f_monitoring == "Geen":
    df = df[df["monitoring_type"] == ""]
if f_search:
    mask = df[["naam","ip_adres","os","beheerder","functie","locatie"]].apply(
        lambda col: col.astype(str).str.contains(f_search, case=False, na=False)
    ).any(axis=1)
    df = df[mask]


# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="background:linear-gradient(135deg,#5b2882 0%,#3d5a9e 50%,#4a8fd9 100%);padding:20px 28px;border-radius:12px;margin-bottom:20px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 2px 12px rgba(91,40,130,0.2)">
  <div>
    <div style="color:rgba(255,255,255,0.5);font-size:10px;text-transform:uppercase;letter-spacing:2px">Server Overzicht</div>
    <div style="color:white;font-family:'Open Sans',sans-serif;font-size:22px;font-weight:800;letter-spacing:-0.3px;margin-top:2px">Prinses Maxima Centrum</div>
    <div style="width:40px;height:2px;background:#c49a2c;border-radius:2px;margin-top:8px"></div>
  </div>
  <div style="text-align:right">
    <div style="color:white;font-size:28px;font-weight:800">{len(df)}<span style="font-size:14px;font-weight:400;color:rgba(255,255,255,0.6)"> / {len(df_all)} servers</span></div>
    <div style="color:rgba(255,255,255,0.4);font-size:11px;margin-top:4px">Data van {NOW.strftime('%d-%m-%Y %H:%M')}</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ─── Aandachtspunten (bovenaan — dit is het waardevolste) ─────────────────────
total = len(df)
aan   = (df["status"] == "poweredOn").sum()
no_bk = df[(df["backup_flag"] == "Nee") & (df["status"] == "poweredOn")]
stale_bk = df[(df["backup_flag"] == "Ja") & (df["dagen_backup"].notna()) & (df["dagen_backup"] > BACKUP_WARN_DAYS)]
no_mon = df[(df["monitoring_type"] == "") & (df["status"] == "poweredOn")]
disk_low = df[(df["min_free_pct"].notna()) & (df["min_free_pct"] < 15)]
tools_bad = df[df["tools_status"].isin(["toolsOld", "toolsNotRunning", "toolsNotInstalled"])]

alert_count = len(no_bk) + len(stale_bk) + len(no_mon) + len(disk_low) + len(tools_bad)

if alert_count > 0:
    items_html = []
    if not no_bk.empty:
        items_html.append(f'<span style="color:#e05a6b;font-weight:700">{len(no_bk)}</span> zonder backup')
    if not stale_bk.empty:
        items_html.append(f'<span style="color:#c49a2c;font-weight:700">{len(stale_bk)}</span> backup verlopen')
    if not no_mon.empty:
        items_html.append(f'<span style="color:#c49a2c;font-weight:700">{len(no_mon)}</span> zonder monitoring')
    if not disk_low.empty:
        items_html.append(f'<span style="color:#e05a6b;font-weight:700">{len(disk_low)}</span> schijf bijna vol')
    if not tools_bad.empty:
        items_html.append(f'<span style="color:#c49a2c;font-weight:700">{len(tools_bad)}</span> VMware Tools')

    st.markdown(f"""
    <div style="background:#fef8f0;border:1px solid #f0e0c0;border-radius:10px;padding:14px 20px;margin-bottom:16px;display:flex;align-items:center;gap:16px">
      <div style="font-size:24px">⚠️</div>
      <div>
        <div style="font-weight:700;font-size:13px;color:#333;margin-bottom:2px">{alert_count} aandachtspunten vereisen actie</div>
        <div style="font-size:12px;color:#666">{'&nbsp;&nbsp;·&nbsp;&nbsp;'.join(items_html)}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ─── KPI rij ──────────────────────────────────────────────────────────────────
bk_pct    = round((df["backup_flag"] == "Ja").sum() / total * 100) if total else 0
mon_count = (df["monitoring_type"] != "").sum()
mon_pct   = round(mon_count / total * 100) if total else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Servers",    f"{aan} / {total}", delta="actief" if aan == total else f"{total - aan} uit")
k2.metric("Backup",     f"{bk_pct}%",       delta=f"{(df['backup_flag'] == 'Ja').sum()} van {total}")
k3.metric("Monitoring", f"{mon_pct}%",       delta=f"{mon_count} van {total}")
k4.metric("Schijf OK",  f"{total - len(disk_low)}", delta=f"{len(disk_low)} kritiek" if len(disk_low) else "alles OK", delta_color="inverse" if len(disk_low) else "normal")
k5.metric("Tools OK",   f"{total - len(tools_bad)}", delta=f"{len(tools_bad)} aandacht" if len(tools_bad) else "alles OK", delta_color="inverse" if len(tools_bad) else "normal")


# ─── Grafieken: alleen de 3 meest actiegerichte ──────────────────────────────
c1, c2, c3 = st.columns(3)

with c1:
    # Locatie verdeling — geeft overzicht waar de servers staan
    loc_counts = df["locatie"].value_counts().reset_index()
    loc_counts.columns = ["locatie", "aantal"]
    fig_loc = px.pie(loc_counts, values="aantal", names="locatie", title="Verdeling per locatie",
                     hole=0.5, color_discrete_sequence=["#5b2882", "#4a8fd9", "#7c5ba8", "#89b4e8"])
    fig_loc.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=240, showlegend=True,
                          legend=dict(orientation="h", y=-0.15, font=dict(size=10)),
                          title_font=dict(size=13, family="Open Sans"), font_family="Open Sans")
    fig_loc.update_traces(textposition="inside", textinfo="value")
    st.plotly_chart(fig_loc, use_container_width=True, key="c_loc")

with c2:
    # Backup + Monitoring gecombineerd — de twee belangrijkste compliance-vragen
    categories = ["Backup", "24x7 Monitoring"]
    ok_vals    = [int((df["backup_flag"] == "Ja").sum()), int(mon_count)]
    nok_vals   = [int((df["backup_flag"] == "Nee").sum()), int(total - mon_count)]
    fig_comp = go.Figure()
    fig_comp.add_trace(go.Bar(name="OK", x=categories, y=ok_vals, marker_color="#48bb78", text=ok_vals, textposition="inside"))
    fig_comp.add_trace(go.Bar(name="Ontbreekt", x=categories, y=nok_vals, marker_color="#e05a6b", text=nok_vals, textposition="inside"))
    fig_comp.update_layout(barmode="stack", margin=dict(t=40, b=0, l=0, r=0), height=240,
                           title="Backup & Monitoring dekking", title_font=dict(size=13, family="Open Sans"),
                           font_family="Open Sans", legend=dict(orientation="h", y=-0.15, font=dict(size=10)),
                           yaxis_title="Servers")
    st.plotly_chart(fig_comp, use_container_width=True, key="c_comp")

with c3:
    # Top 8 schijfruimte — direct actiegericht
    disk_df = df[df["min_free_pct"].notna()].copy()
    disk_df["min_free_pct"] = pd.to_numeric(disk_df["min_free_pct"], errors="coerce")
    disk_top = disk_df.nsmallest(8, "min_free_pct")[["naam", "min_free_pct"]].copy()
    if not disk_top.empty:
        disk_top["min_free_pct"] = disk_top["min_free_pct"].astype(int)
        fig_disk = px.bar(disk_top, y="naam", x="min_free_pct", orientation="h",
                          title="Laagste schijfruimte (% vrij)", text="min_free_pct",
                          color="min_free_pct", color_continuous_scale=["#e05a6b", "#c49a2c", "#48bb78"],
                          range_color=[0, 100])
        fig_disk.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=240, showlegend=False,
                               title_font=dict(size=13, family="Open Sans"), font_family="Open Sans",
                               xaxis_title="% vrij", yaxis_title="", coloraxis_showscale=False,
                               yaxis=dict(autorange="reversed"))
        fig_disk.update_traces(texttemplate="%{text}%", textposition="outside")
        st.plotly_chart(fig_disk, use_container_width=True, key="c_disk")
    else:
        st.info("Geen schijfdata beschikbaar")


# ─── Detaillijst aandachtspunten (inklapbaar) ─────────────────────────────────
if alert_count > 0:
    with st.expander(f"Details: {alert_count} aandachtspunten", expanded=False):
        a1, a2, a3 = st.columns(3)
        with a1:
            if not no_bk.empty:
                st.markdown(f'<div class="alert-box"><strong>{len(no_bk)} servers zonder backup</strong><br>' +
                            "<br>".join(f"• {n}" for n in no_bk["naam"].tolist()[:10]) +
                            (f"<br>…en {len(no_bk)-10} meer" if len(no_bk) > 10 else "") +
                            "</div>", unsafe_allow_html=True)
            if not disk_low.empty:
                st.markdown(f'<div class="alert-box"><strong>{len(disk_low)} servers schijf &lt;15%</strong><br>' +
                            "<br>".join(f"• {r['naam']} ({int(r['min_free_pct'])}%)" for _, r in disk_low.iterrows()) +
                            "</div>", unsafe_allow_html=True)
        with a2:
            if not stale_bk.empty:
                st.markdown(f'<div class="warn-box"><strong>{len(stale_bk)} backups verlopen</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in stale_bk.head(10).iterrows()) +
                            "</div>", unsafe_allow_html=True)
            if not tools_bad.empty:
                lm = {"toolsOld": "Verouderd", "toolsNotRunning": "Niet actief", "toolsNotInstalled": "Niet geïnst."}
                st.markdown(f'<div class="warn-box"><strong>{len(tools_bad)} VMware Tools</strong><br>' +
                            "<br>".join(f"• {r['naam']} ({lm.get(r['tools_status'], '')})" for _, r in tools_bad.iterrows()) +
                            "</div>", unsafe_allow_html=True)
        with a3:
            if not no_mon.empty:
                st.markdown(f'<div class="warn-box"><strong>{len(no_mon)} zonder 24x7 monitoring</strong><br>' +
                            "<br>".join(f"• {r['naam']}" for _, r in no_mon.head(12).iterrows()) +
                            (f"<br>…en {len(no_mon)-12} meer" if len(no_mon) > 12 else "") +
                            "</div>", unsafe_allow_html=True)


st.divider()


# ─── Server tabel ─────────────────────────────────────────────────────────────
tbl_left, tbl_right = st.columns([4, 1])
with tbl_left:
    st.markdown(f"### Servers")
with tbl_right:
    if not df.empty:
        export_cols = ["naam","status","locatie","beheerder","os","monitoring_type","backup_flag",
                       "min_free_pct","cpu_pct","mem_pct","ip_adres","vcpu","geheugen_gib","functie"]
        df_export = df[[c for c in export_cols if c in df.columns]].copy()
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="Servers")
        st.download_button("📥 Excel", data=buf.getvalue(),
                           file_name=f"PMC_servers_{NOW.strftime('%Y%m%d')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)

# Compacte tabel: alleen de kolommen die ertoe doen
def fmt_status(s):
    return {"poweredOn": "✅ Aan", "poweredOff": "❌ Uit", "suspended": "⏸ Susp."}.get(s, s)

def fmt_mon(t):
    if not t: return "–"
    return "🟣 SCOM" if t == "SCOM" else ("🔵 Nagios" if t == "Nagios" else t)

def fmt_disk(val):
    if val is None or (isinstance(val, float) and pd.isna(val)): return "–"
    v = int(val)
    return f"{'🔴' if v < 15 else '🟠' if v < 30 else '🟢'} {v}%"

def fmt_cpu(val):
    if val is None or (isinstance(val, float) and pd.isna(val)): return "–"
    v = int(val)
    return f"{'🔴' if v > 90 else '🟠' if v > 70 else '🟢'} {v}%"

tbl = df[["naam","status","locatie","beheerder","monitoring_type","backup_flag",
          "min_free_pct","cpu_pct","mem_pct","ip_adres","functie"]].copy()

tbl["status"]          = tbl["status"].map(fmt_status)
tbl["monitoring_type"] = tbl["monitoring_type"].apply(fmt_mon)
tbl["backup_flag"]     = tbl["backup_flag"].map({"Ja": "✅", "Nee": "❌"})
tbl["min_free_pct"]    = df["min_free_pct"].apply(fmt_disk)
tbl["cpu_pct"]         = df["cpu_pct"].apply(fmt_cpu)
tbl["mem_pct"]         = df["mem_pct"].apply(fmt_cpu)  # same thresholds
tbl["specs"]           = df.apply(lambda r: f"{r['vcpu']}c / {r['geheugen_gib']}G", axis=1) if not df.empty else pd.Series([], dtype=str)

tbl_show = tbl.rename(columns={
    "naam": "Server", "status": "Status", "locatie": "Locatie", "beheerder": "Beheerder",
    "monitoring_type": "24x7", "backup_flag": "BU", "min_free_pct": "Schijf",
    "cpu_pct": "CPU", "mem_pct": "RAM", "ip_adres": "IP", "functie": "Functie", "specs": "Specs"
})

event = st.dataframe(
    tbl_show, use_container_width=True, height=480, hide_index=True,
    on_select="rerun", selection_mode="single-row",
    column_config={
        "Server":   st.column_config.TextColumn(width="medium"),
        "Status":   st.column_config.TextColumn(width="small"),
        "Locatie":  st.column_config.TextColumn(width="medium"),
        "24x7":     st.column_config.TextColumn(width="small"),
        "BU":       st.column_config.TextColumn(width="small"),
        "Schijf":   st.column_config.TextColumn(width="small"),
        "CPU":      st.column_config.TextColumn(width="small"),
        "RAM":      st.column_config.TextColumn(width="small"),
        "IP":       st.column_config.TextColumn(width="small"),
        "Specs":    st.column_config.TextColumn(width="small"),
        "Functie":  st.column_config.TextColumn(width="medium"),
    }
)


# ─── SQL Licentie overzicht ───────────────────────────────────────────────────
sql_vms = df_all[df_all["sql_lic_edition"] != ""].copy()
if not sql_vms.empty:
    total_cores = sql_vms["vcpu"].sum()
    editions = sql_vms["sql_lic_edition"].value_counts()

    with st.expander(f"🗄️ SQL Licenties via RAM IT ({len(sql_vms)} servers · {total_cores} cores)", expanded=False):
        sq1, sq2 = st.columns([2, 1])
        with sq1:
            sql_tbl = sql_vms[["naam","functie","sql_lic_version","sql_lic_edition","vcpu","status"]].copy()
            sql_tbl["status"] = sql_tbl["status"].map({"poweredOn": "✅", "poweredOff": "❌"})
            sql_tbl = sql_tbl.rename(columns={
                "naam": "Server", "functie": "Functie", "sql_lic_version": "SQL Versie",
                "sql_lic_edition": "Editie", "vcpu": "Cores", "status": ""
            })
            st.dataframe(sql_tbl, use_container_width=True, hide_index=True, height=min(len(sql_tbl) * 40 + 40, 440))
        with sq2:
            st.markdown("**Samenvatting**")
            for ed, count in editions.items():
                cores = int(sql_vms[sql_vms["sql_lic_edition"] == ed]["vcpu"].sum())
                st.write(f"**{ed}:** {count}x ({cores} cores)")
            st.divider()
            st.metric("Totaal cores", total_cores)
            st.caption("Bron: Product Diensten Rapport RAM IT")


# ─── SCOM vs vCenter mismatch ────────────────────────────────────────────────
if FILE_MONITOR.exists():
    vcenter_vms = set(df_all["naam"].str.upper().tolist())
    monitor_vms_set = set()
    df_mon_check = pd.read_excel(FILE_MONITOR, sheet_name="Export", engine="openpyxl")
    monitor_details = {}
    for _, r in df_mon_check.iterrows():
        server = str(r.get("Server", "") or "")
        if server:
            short = server.split(".")[0].upper()
            monitor_vms_set.add(short)
            monitor_details[short] = {"fqdn": server, "functie": str(r.get("Functie server", "") or "")}

    in_scom_not_vcenter = monitor_vms_set - vcenter_vms
    powered_on_no_scom = [vm for vm in df_all[df_all["status"] == "poweredOn"]["naam"].tolist() if vm.upper() not in monitor_vms_set]

    mismatch_count = len(in_scom_not_vcenter) + len(powered_on_no_scom)
    if mismatch_count:
        with st.expander(f"🔍 Monitoring vs Infra vergelijking ({mismatch_count} mismatches)"):
            m1, m2 = st.columns(2)
            with m1:
                if in_scom_not_vcenter:
                    st.markdown(f"**In SCOM/Nagios, niet in vCenter ({len(in_scom_not_vcenter)})**")
                    for s in sorted(in_scom_not_vcenter):
                        d = monitor_details.get(s, {})
                        st.write(f"• {d.get('fqdn', s)} — {d.get('functie', '')}")
                else:
                    st.success("Alle gemonitorde servers staan in vCenter")
            with m2:
                if powered_on_no_scom:
                    st.markdown(f"**Actief in vCenter, geen monitoring ({len(powered_on_no_scom)})**")
                    for s in sorted(powered_on_no_scom)[:15]:
                        st.write(f"• {s}")
                    if len(powered_on_no_scom) > 15:
                        st.caption(f"…en {len(powered_on_no_scom) - 15} meer")
                else:
                    st.success("Alle actieve servers zijn gemonitord")


# ─── Detailpaneel ────────────────────────────────────────────────────────────
selected_rows = event.selection.rows if event and event.selection else []

if selected_rows:
    idx = df.index[selected_rows[0]]
    vm  = df.loc[idx]

    st.divider()
    # Compact detail header
    status_color = "#48bb78" if vm["status"] == "poweredOn" else "#e05a6b"
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">
      <div style="width:10px;height:10px;border-radius:50%;background:{status_color}"></div>
      <div style="font-size:20px;font-weight:800;color:#333">{vm['naam']}</div>
      <div style="font-size:12px;color:#888;background:#f5f0fa;padding:2px 10px;border-radius:12px">{vm['locatie']}</div>
      <div style="font-size:12px;color:#888;background:#f5f0fa;padding:2px 10px;border-radius:12px">{vm['beheerder'] or '–'}</div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Overzicht", "Opslag", "Backup & Reboot", "Tools & Monitoring", "Netwerk"])

    with tab1:
        d1, d2, d3 = st.columns(3)
        with d1:
            st.markdown("**Infra**")
            st.write(f"**Datacenter:** {vm['datacenter']}")
            st.write(f"**Cluster:** {vm['cluster']}")
            st.write(f"**ESXi Host:** {vm['esxi_host']}")
            st.write(f"**Aangemaakt:** {vm['aanmaakdatum']}")
            st.write(f"**OS:** {vm['os'] or '–'}")
        with d2:
            st.markdown("**Performance**")
            if vm['cpu_pct'] is not None and not (isinstance(vm['cpu_pct'], float) and pd.isna(vm['cpu_pct'])):
                st.write(f"**CPU:** {fmt_cpu(vm['cpu_pct'])} ({vm['cpu_overall_mhz']} / {vm['cpu_max_mhz']} MHz)")
            else:
                st.write("**CPU:** –")
            st.write(f"**vCPU:** {vm['vcpu']}  ·  **RAM:** {vm['geheugen_gib']} GB")
            if vm['mem_pct'] is not None and not (isinstance(vm['mem_pct'], float) and pd.isna(vm['mem_pct'])):
                st.write(f"**RAM gebruik:** {fmt_cpu(vm['mem_pct'])} ({vm['mem_consumed']} / {vm['mem_size']} MiB)")
                if vm['mem_swapped'] and vm['mem_swapped'] > 0:
                    st.warning(f"Swapped: {vm['mem_swapped']} MiB")
                if vm['mem_ballooned'] and vm['mem_ballooned'] > 0:
                    st.warning(f"Ballooned: {vm['mem_ballooned']} MiB")
        with d3:
            st.markdown("**Beheer**")
            st.write(f"**Klantnaam:** {vm['klantnaam'] or '–'}")
            st.write(f"**Contract:** {vm['contract_nr'] or '–'}")
            st.write(f"**Functie:** {vm['functie'] or '–'}")
            st.write(f"**Soort:** {vm['soort'] or '–'}")
            if vm['sql_lic_edition']:
                st.write(f"**SQL:** {vm['sql_lic_version']} ({vm['sql_lic_edition']})")
            elif vm['sql_versie']:
                st.write(f"**SQL:** {vm['sql_versie']} ({vm['sql_editie']})")
            st.write(f"**HA:** {'✅ Ja' if vm['ha_beschermd'] else '–'}")

    with tab2:
        partitions = vm['partitions'] if isinstance(vm['partitions'], list) else []
        if partitions:
            for p in partitions:
                cap  = p['capacity_mib']
                free = p['free_pct']
                pct  = round((1 - free / 100) * 100) if cap else 0
                color = "#48bb78" if free >= 30 else ("#c49a2c" if free >= 15 else "#e05a6b")
                st.markdown(f"""
                <div style="margin-bottom:8px">
                  <div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;margin-bottom:2px">
                    <span>{p['disk'] or '?'}</span>
                    <span>{free}% vrij ({round(p['free_mib']/1024, 1)} / {round(cap/1024, 1)} GB)</span>
                  </div>
                  <div style="background:#eee;border-radius:4px;height:10px;overflow:hidden">
                    <div style="background:{color};height:100%;width:{pct}%;border-radius:4px"></div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("Geen partitie-data beschikbaar")

    with tab3:
        b1, b2 = st.columns(2)
        with b1:
            st.write(f"**Backup:** {'✅ Ja' if vm['backup_flag'] == 'Ja' else '❌ Nee'}")
            backup_dt_str = fmt_date(vm['backup_datum'])
            if backup_dt_str:
                d = vm['dagen_backup']
                st.write(f"**Laatste:** {'🟢' if d <= BACKUP_WARN_DAYS else '🔴'} {backup_dt_str} ({d}d geleden)")
            if vm['backup_str']:
                st.code(vm['backup_str'], language=None)
        with b2:
            reboot_str = fmt_date(vm['laatste_reboot'])
            if reboot_str:
                d = vm['dagen_reboot'] or 0
                st.write(f"**Laatste reboot:** {'🟢' if d <= REBOOT_WARN_DAYS else '🟠'} {reboot_str} ({d}d)")
            else:
                st.write("**Laatste reboot:** –")
            host_boot_str = fmt_date(vm['host_boot'])
            if host_boot_str:
                d = vm['dagen_host_boot'] or 0
                st.write(f"**Host boot:** {'🟢' if d <= REBOOT_WARN_DAYS else '🟠'} {host_boot_str} ({d}d)")
            st.write(f"**Patch schema:** {vm['update_moment'] or '–'}")

    with tab4:
        t1, t2 = st.columns(2)
        with t1:
            st.write(f"**VMware Tools:** {TOOLS_LABELS.get(vm['tools_status'], vm['tools_status'])}")
            st.write(f"**Versie:** {vm['tools_versie'] or '–'}  ·  **Upgrade:** {vm['tools_upgradeable'] or '–'}")
            if vm['heeft_snapshot']:
                st.warning("Actieve snapshot aanwezig")
        with t2:
            if vm['monitoring_type']:
                st.write(f"**24x7:** {fmt_mon(vm['monitoring_type'])}")
                if vm['monitoring_functie']:
                    st.write(f"**Registratie:** {vm['monitoring_functie']}")
            else:
                st.write("**24x7:** – Geen monitoring")
            health = vm['health_messages'] if isinstance(vm['health_messages'], list) else []
            if health:
                for msg in health:
                    st.warning(msg)

    with tab5:
        nics = vm['nics'] if isinstance(vm['nics'], list) else []
        if nics:
            nic_df = pd.DataFrame(nics)
            nic_df.columns = ["NIC", "VLAN", "Switch", "Connected", "MAC", "IPv4"]
            nic_df["Connected"] = nic_df["Connected"].map({True: "✅", False: "❌"})
            st.dataframe(nic_df, use_container_width=True, hide_index=True)
        else:
            st.caption("Geen netwerkdata beschikbaar")
        st.write(f"**IP (primair):** {vm['ip_adres'] or '–'}")
        if vm['wiki_link']:
            st.markdown(f"[Wiki link]({vm['wiki_link']})")

else:
    st.caption("Klik op een server in de tabel voor details")


# ─── Footer ──────────────────────────────────────────────────────────────────
st.divider()
fc1, fc2 = st.columns(2)
with fc1:
    st.caption(f"ram infotechnology · Prinses Maxima Centrum")
with fc2:
    st.caption(f"Brondata: vCenter PMC + PPD/GK · SCOM/Nagios · SQL licenties · {NOW.strftime('%d-%m-%Y')}")
