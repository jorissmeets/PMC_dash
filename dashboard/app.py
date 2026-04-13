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
FILE_R7_VULNS  = BASE_DIR / "rapid7_vulns.csv"
FILE_R7_ASSETS = BASE_DIR / "rapid7_assets.csv"
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

    # ── Rapid7 vulnerability + asset data ────────────────────────────────────
    def _safe_int(v):
        try: return int(float(v)) if pd.notna(v) else 0
        except: return 0
    def _safe_float(v):
        try: return float(v) if pd.notna(v) else 0.0
        except: return 0.0

    r7_map = {}
    r7_scanned_hosts = set()

    # Assets tabel: alle gescande hosts (ook zonder vulns)
    if FILE_R7_ASSETS.exists():
        df_r7a = pd.read_csv(FILE_R7_ASSETS)
        for _, r in df_r7a.iterrows():
            hostname = str(r.get("Hostname", "") or "").split(".")[0].upper()
            if hostname and hostname != "ONBEKEND":
                r7_scanned_hosts.add(hostname)
                r7_map[hostname] = {"vuln_total": 0, "vuln_critical": 0, "vuln_high": 0,
                                    "vuln_medium": 0, "max_cvss": 0.0, "last_scan": ""}

    # Vulns tabel: overschrijf met echte vuln data
    if FILE_R7_VULNS.exists():
        df_r7 = pd.read_csv(FILE_R7_VULNS)
        for _, r in df_r7.iterrows():
            hostname = str(r.get("AssetHostname", "") or "").split(".")[0].upper()
            if hostname:
                r7_scanned_hosts.add(hostname)
                r7_map[hostname] = {
                    "vuln_total": _safe_int(r.get("vuln_total")),
                    "vuln_critical": _safe_int(r.get("critical")),
                    "vuln_high": _safe_int(r.get("high")),
                    "vuln_medium": _safe_int(r.get("medium")),
                    "max_cvss": _safe_float(r.get("max_cvss")),
                    "last_scan": str(r.get("last_scan", "") or ""),
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
            "vuln_total":        r7_map.get(vm.upper(), {}).get("vuln_total", 0),
            "vuln_critical":     r7_map.get(vm.upper(), {}).get("vuln_critical", 0),
            "vuln_high":         r7_map.get(vm.upper(), {}).get("vuln_high", 0),
            "max_cvss":          r7_map.get(vm.upper(), {}).get("max_cvss", 0),
            "r7_scanned":        vm.upper() in r7_map,
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
            "vuln_total":        r7_map.get(str(r.get("VM", "")).upper(), {}).get("vuln_total", 0),
            "vuln_critical":     r7_map.get(str(r.get("VM", "")).upper(), {}).get("vuln_critical", 0),
            "vuln_high":         r7_map.get(str(r.get("VM", "")).upper(), {}).get("vuln_high", 0),
            "max_cvss":          r7_map.get(str(r.get("VM", "")).upper(), {}).get("max_cvss", 0),
            "r7_scanned":        str(r.get("VM", "")).upper() in r7_map,
        })

    df2 = pd.DataFrame(records2)
    return pd.concat([df1, df2], ignore_index=True)


# ─── Pagina configuratie ───────────────────────────────────────────────────────
st.set_page_config(page_title="Prinses Maxima – Serverdienstverlening", page_icon="🖥️", layout="wide", initial_sidebar_state="expanded")

# ─── Login ─────────────────────────────────────────────────────────────────────
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown("""
    <style>
      [data-testid="stSidebar"] { display: none; }
      .stApp { background: linear-gradient(135deg, #5b2882 0%, #3d5a9e 50%, #4a8fd9 100%) !important; }
      [data-testid="stMainBlockContainer"] { display: flex; align-items: center; justify-content: center; min-height: 80vh; }
    </style>
    """, unsafe_allow_html=True)
    spacer_l, card, spacer_r = st.columns([1, 1.2, 1])
    with card:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:40px 36px 32px 36px;box-shadow:0 8px 32px rgba(0,0,0,0.18);text-align:center">
          <div style="font-family:'Open Sans',sans-serif;font-size:24px;letter-spacing:-0.5px;margin-bottom:4px">
            <span style="font-weight:300;color:#5b2882">ram</span> <span style="font-weight:800;color:#5b2882">infotechnology</span>
          </div>
          <div style="width:36px;height:3px;background:#c49a2c;border-radius:2px;margin:10px auto 20px auto"></div>
          <div style="color:#888;font-size:13px;margin-bottom:4px">Serverdienstverlening</div>
          <div style="color:#aaa;font-size:12px">Prinses Maxima Centrum</div>
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
  [data-testid="stMetricValue"] { font-size: 1.6rem !important; font-weight: 800 !important; color: #5b2882 !important; }
  [data-testid="stMetricLabel"] { font-size: 0.6rem !important; text-transform: uppercase; letter-spacing: 0.6px; color: #888 !important; }
  [data-testid="stSidebar"] { background: linear-gradient(180deg, #5b2882 0%, #3d5a9e 60%, #4a8fd9 100%) !important; }
  [data-testid="stSidebar"] * { color: white !important; }
  [data-testid="stSidebar"] label { color: rgba(255,255,255,0.6) !important; font-size: 10px !important; text-transform: uppercase; letter-spacing: 0.5px; }
  [data-testid="stSidebar"] .stSelectbox > div > div { background: rgba(255,255,255,0.1) !important; border: 1px solid rgba(255,255,255,0.15) !important; }
  [data-testid="stSidebar"] input { background: rgba(255,255,255,0.1) !important; border: 1px solid rgba(255,255,255,0.15) !important; }
  [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.1) !important; }
  div[data-testid="stExpander"] summary { font-weight: 600; }
  div[data-testid="stExpander"] { border: 1px solid #ede8f3 !important; border-radius: 10px !important; }
  .stButton > button { background: linear-gradient(135deg, #5b2882, #4a8fd9) !important; color: white !important; border: none !important; font-weight: 600 !important; border-radius: 8px !important; }
  .stButton > button:hover { background: linear-gradient(135deg, #4a2070, #3d7bc4) !important; }
  .stDownloadButton > button { background: white !important; color: #5b2882 !important; border: 2px solid #5b2882 !important; font-weight: 600 !important; border-radius: 8px !important; font-size: 12px !important; }
  .stTabs [data-baseweb="tab"] { background: #f8f5fb; border-radius: 8px 8px 0 0; }
  .stTabs [aria-selected="true"] { background: #5b2882 !important; color: white !important; }
  hr { border-color: #ede8f3 !important; }
  .src-pill { display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600;margin-right:6px; }
  .src-green  { background:#e6f7e9;color:#276749; }
  .src-orange { background:#fef3e0;color:#8a6d20; }
  .src-grey   { background:#f0f0f0;color:#888; }
</style>
""", unsafe_allow_html=True)

df_all = load_data()

# ─── Risicoscore berekenen ───────────────────────────────────────────────────
RISK_HIGH_THRESHOLD = 5

def calc_risk(r):
    """Risicoscore op schaal 0-10. Retourneert (score, breakdown_tekst).
    Weging: backup 3, schijf 3, tools 2, reboot 1(+1), monitoring 1 = max 10.
    Exclusieve condities: backup ontbreekt OF verlopen (nooit beide), schijf <15 OF 15-30 (nooit beide)."""
    score = 0
    parts = []
    if r["status"] == "poweredOn":
        # Backup: 0 of 2 of 3 (exclusief)
        if r["backup_flag"] == "Nee":
            score += 3; parts.append("Geen backup (+3)")
        elif r["dagen_backup"] is not None and r["dagen_backup"] > BACKUP_WARN_DAYS:
            score += 2; parts.append(f"Backup verlopen {int(r['dagen_backup'])}d (+2)")
        # Schijf: 0 of 1 of 3 (exclusief)
        if r["min_free_pct"] is not None and r["min_free_pct"] < 15:
            score += 3; parts.append(f"Schijf {int(r['min_free_pct'])}% (+3)")
        elif r["min_free_pct"] is not None and r["min_free_pct"] < 30:
            score += 1; parts.append(f"Schijf {int(r['min_free_pct'])}% (+1)")
        # Tools: 0 of 2
        if r["tools_status"] in ("toolsOld","toolsNotRunning","toolsNotInstalled"):
            score += 2; parts.append("VMware Tools (+2)")
        # Reboot: 0 of 1 of 2 (exclusief)
        if r["dagen_reboot"] is not None and r["dagen_reboot"] > 365:
            score += 2; parts.append(f"Reboot {int(r['dagen_reboot'])}d (+2)")
        elif r["dagen_reboot"] is not None and r["dagen_reboot"] > REBOOT_WARN_DAYS:
            score += 1; parts.append(f"Reboot {int(r['dagen_reboot'])}d (+1)")
        # Monitoring: 0 of 1
        if r["monitoring_type"] == "":
            score += 1; parts.append("Geen monitoring (+1)")
    return score, " | ".join(parts) if parts else "Geen risico's"

risk_results = df_all.apply(calc_risk, axis=1, result_type="expand")
risk_results.columns = ["risico", "risico_detail"]
df_all["risico"] = risk_results["risico"]
df_all["risico_detail"] = risk_results["risico_detail"]

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""<div style="padding:4px 0 12px 0">
      <div style="color:white;font-family:'Open Sans',sans-serif;font-size:20px;letter-spacing:-0.5px">
        <span style="font-weight:300">ram</span> <span style="font-weight:800">infotechnology</span></div>
      <div style="width:30px;height:2px;background:#c49a2c;border-radius:2px;margin-top:6px"></div>
    </div>""", unsafe_allow_html=True)
    if st.button("🔄 Ververs", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    f_search     = st.text_input("🔍 Zoeken", placeholder="Server, IP, OS…")
    f_locatie    = st.selectbox("Locatie",     ["Alle"] + sorted(df_all["locatie"].dropna().unique().tolist()))
    f_status     = st.selectbox("Status",      ["Alle", "Aan", "Uit"])
    f_beheerder  = st.selectbox("Beheerder",   ["Alle"] + sorted([b for b in df_all["beheerder"].dropna().unique().tolist() if b]))
    f_backup     = st.selectbox("Backup",      ["Alle", "Ja", "Nee"])
    f_monitoring = st.selectbox("Monitoring",  ["Alle", "SCOM", "Nagios", "Geen"])
    f_risico     = st.selectbox("Risico",      ["Alle", "Alleen risico's"])

# ─── Filters ─────────────────────────────────────────────────────────────────
df = df_all.copy()
if f_locatie != "Alle":    df = df[df["locatie"] == f_locatie]
if f_status == "Aan":      df = df[df["status"] == "poweredOn"]
elif f_status == "Uit":    df = df[df["status"] == "poweredOff"]
if f_beheerder != "Alle":  df = df[df["beheerder"] == f_beheerder]
if f_backup == "Ja":       df = df[df["backup_flag"] == "Ja"]
elif f_backup == "Nee":    df = df[df["backup_flag"] == "Nee"]
if f_monitoring == "SCOM":   df = df[df["monitoring_type"] == "SCOM"]
elif f_monitoring == "Nagios": df = df[df["monitoring_type"] == "Nagios"]
elif f_monitoring == "Geen": df = df[df["monitoring_type"] == ""]
if f_risico == "Alleen risico's": df = df[df["risico"] > 0]
if f_search:
    mask = df[["naam","ip_adres","os","beheerder","functie","locatie"]].apply(
        lambda col: col.astype(str).str.contains(f_search, case=False, na=False)).any(axis=1)
    df = df[mask]

total = len(df)

# ═══════════════════════════════════════════════════════════════════════════════
#  HEADER — compact
# ═══════════════════════════════════════════════════════════════════════════════
h1, h2 = st.columns([3, 1])
with h1:
    st.markdown(f"""<div style="margin-bottom:4px">
      <span style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1.5px">Serverdienstverlening</span><br>
      <span style="font-size:22px;font-weight:800;color:#333">Prinses Maxima Centrum</span>
    </div>""", unsafe_allow_html=True)
with h2:
    st.markdown(f"""<div style="text-align:right">
      <span style="font-size:28px;font-weight:800;color:#5b2882">{total}</span>
      <span style="font-size:13px;color:#888"> / {len(df_all)} servers</span><br>
      <span style="font-size:11px;color:#aaa">Peildatum {NOW.strftime('%d-%m-%Y %H:%M')}</span>
    </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  DATABRONNEN STATUSBALK
# ═══════════════════════════════════════════════════════════════════════════════
bk_count  = int((df_all["backup_flag"] == "Ja").sum())
mon_count = int((df_all["monitoring_type"] != "").sum())
total_all = len(df_all)

r7_scanned_count = int(df_all["r7_scanned"].sum())
r7_has_data = FILE_R7_VULNS.exists()
src_items = [
    ("vCenter",     f"{total_all}/{total_all} geladen",    "src-green"),
    ("Backup",      f"{bk_count}/{total_all} dekking",     "src-green" if bk_count == total_all else "src-orange"),
    ("24x7 Monitoring", f"{mon_count}/{total_all} gedekt (alleen RAM-DC)", "src-orange"),
    ("Rapid7",      f"{r7_scanned_count}/{total_all} gescand" if r7_has_data else "Nog niet gekoppeld",
                    "src-orange" if r7_has_data and r7_scanned_count < total_all else ("src-green" if r7_has_data else "src-grey")),
    ("TopDesk",     "Nog niet gekoppeld",                  "src-grey"),
]
pills = " ".join(f'<span class="src-pill {cls}">{name}: {txt}</span>' for name, txt, cls in src_items)
st.markdown(f'<div style="margin:4px 0 16px 0">{pills}</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  KPI CARDS — 6 stuks met eerlijke labels
# ═══════════════════════════════════════════════════════════════════════════════
disk_crit = df[(df["min_free_pct"].notna()) & (df["min_free_pct"] < 15)]
tools_bad = df[df["tools_status"].isin(["toolsOld","toolsNotRunning","toolsNotInstalled"])]
risk_count = (df["risico"] > 0).sum()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Servers in scope",        total)
k2.metric("Backup compliant",        f"{int((df['backup_flag'] == 'Ja').sum())}/{total}")
k3.metric("24x7 monitoring gedekt",  f"{int((df['monitoring_type'] != '').sum())}/{total}")
k4.metric("Schijfruimte kritiek",    len(disk_crit), delta="servers <15% vrij" if len(disk_crit) else "OK", delta_color="inverse" if len(disk_crit) else "normal")
r7_clean = int(((df["r7_scanned"]) & (df["vuln_critical"] == 0) & (df["vuln_high"] == 0)).sum())
r7_total = int(df["r7_scanned"].sum())
if r7_total > 0:
    k5.metric("Patch compliant", f"{r7_clean}/{r7_total}", delta=f"{r7_total - r7_clean} met kwetsbaarheden" if r7_clean < r7_total else "alles OK",
              delta_color="inverse" if r7_clean < r7_total else "normal")
else:
    k5.metric("Patch compliant", "–", delta="Rapid7 bron ontbreekt")
k6.metric("Open risico's",           risk_count, delta="servers met score >0" if risk_count else "alles OK", delta_color="inverse" if risk_count else "normal")

# ═══════════════════════════════════════════════════════════════════════════════
#  TWEE BLOKKEN: Actie nodig (links) + Datadekking (rechts)
# ═══════════════════════════════════════════════════════════════════════════════
act_col, data_col = st.columns(2)

with act_col:
    st.markdown("**Open risico's — directe actie nodig**")
    risk_servers = df[df["risico"] > 0].nlargest(10, "risico")
    if not risk_servers.empty:
        risk_rows = []
        for _, r in risk_servers.iterrows():
            risk_rows.append({"Server": r["naam"], "Issues": r["risico_detail"], "Score": int(r["risico"]),
                              "Beheerder": r["beheerder"] or "–", "Locatie": r["locatie"]})
        st.dataframe(pd.DataFrame(risk_rows), use_container_width=True, hide_index=True, height=min(len(risk_rows)*38+40, 400))
    else:
        st.success("Geen open risico's")

with data_col:
    st.markdown("**Datadekking — ontbrekende informatie**")
    no_beh = int((df_all["beheerder"] == "").sum())
    no_part = int(df_all["min_free_pct"].isna().sum())
    cov_items = []
    cov_items.append(f'<div style="padding:6px 0;border-bottom:1px solid #f0f0f0"><span style="color:#48bb78;font-weight:700">{bk_count}</span> / {total_all} servers met backup record</div>')
    cov_items.append(f'<div style="padding:6px 0;border-bottom:1px solid #f0f0f0"><span style="color:#c49a2c;font-weight:700">{mon_count}</span> / {total_all} met monitoring record <span style="color:#aaa;font-size:11px">(bron dekt alleen RAM-DC)</span></div>')
    cov_items.append(f'<div style="padding:6px 0;border-bottom:1px solid #f0f0f0"><span style="color:#aaa;font-weight:700">0</span> / {total_all} met patchbron <span style="color:#aaa;font-size:11px">(Rapid7 nog niet gekoppeld)</span></div>')
    cov_items.append(f'<div style="padding:6px 0;border-bottom:1px solid #f0f0f0"><span style="color:#c49a2c;font-weight:700">{no_beh}</span> servers zonder beheerder ingevuld</div>')
    cov_items.append(f'<div style="padding:6px 0"><span style="color:#c49a2c;font-weight:700">{no_part}</span> servers zonder schijfdata <span style="color:#aaa;font-size:11px">(alleen PMC on-prem beschikbaar)</span></div>')
    st.markdown(f'<div style="background:#fafafa;border:1px solid #eee;border-radius:8px;padding:8px 14px;font-size:13px">{"".join(cov_items)}</div>', unsafe_allow_html=True)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
#  VISUALS — compliance bars + risico chart + issues per locatie
# ═══════════════════════════════════════════════════════════════════════════════
v1, v2, v3 = st.columns(3)

with v1:
    # Compliance per domein — stacked horizontal bars
    r7_ok   = int(((df["r7_scanned"]) & (df["vuln_critical"]==0) & (df["vuln_high"]==0)).sum())
    r7_bad  = int(((df["r7_scanned"]) & ((df["vuln_critical"]>0) | (df["vuln_high"]>0))).sum())
    r7_miss = int((~df["r7_scanned"]).sum())
    domains = ["Backup", "Monitoring", "Schijf", "Patching"]
    compliant = [int((df["backup_flag"]=="Ja").sum()), int((df["monitoring_type"]!="").sum()),
                 int(((df["min_free_pct"].isna()) | (df["min_free_pct"]>=15)).sum()) - int(df["min_free_pct"].isna().sum()), r7_ok]
    missing   = [0, 0, int(df["min_free_pct"].isna().sum()), r7_miss]
    non_comp  = [int((df["backup_flag"]=="Nee").sum()), 0, int(((df["min_free_pct"].notna()) & (df["min_free_pct"]<15)).sum()), r7_bad]
    # monitoring non-compliant is 0 because missing data, not a failure
    missing[1] = total - int((df["monitoring_type"]!="").sum())

    fig_c = go.Figure()
    fig_c.add_trace(go.Bar(name="Compliant", y=domains, x=compliant, orientation="h", marker_color="#48bb78", text=compliant, textposition="inside"))
    fig_c.add_trace(go.Bar(name="Data ontbreekt", y=domains, x=missing, orientation="h", marker_color="#f0d080", text=[m if m else "" for m in missing], textposition="inside"))
    fig_c.add_trace(go.Bar(name="Non-compliant", y=domains, x=non_comp, orientation="h", marker_color="#e05a6b", text=[n if n else "" for n in non_comp], textposition="inside"))
    fig_c.update_layout(barmode="stack", margin=dict(t=40,b=0,l=0,r=0), height=220,
                         title="Compliance per domein", title_font=dict(size=13, family="Open Sans"),
                         font_family="Open Sans", legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
                         xaxis_title="Servers", yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_c, use_container_width=True, key="v_comp")

with v2:
    # Top 10 risico-servers
    risk_top = df[df["risico"] > 0].nlargest(10, "risico")[["naam","risico"]].copy()
    if not risk_top.empty:
        risk_top["detail"] = df[df["risico"]>0].nlargest(10,"risico")["risico_detail"].values[:len(risk_top)]
        fig_r = px.bar(risk_top, y="naam", x="risico", orientation="h", title="Top risico-servers",
                       text="risico", hover_data={"detail": True, "risico": True, "naam": False},
                       color="risico", color_continuous_scale=["#c49a2c", "#e05a6b"], range_color=[0, 10])
        fig_r.update_layout(margin=dict(t=40,b=0,l=0,r=0), height=220, showlegend=False,
                            title_font=dict(size=13, family="Open Sans"), font_family="Open Sans",
                            xaxis_title="Risicoscore (0-10)", yaxis_title="", coloraxis_showscale=False,
                            xaxis=dict(range=[0,10]),
                            yaxis=dict(autorange="reversed"))
        fig_r.update_traces(textposition="outside")
        st.plotly_chart(fig_r, use_container_width=True, key="v_risk")
    else:
        st.success("Geen risico-servers")

with v3:
    # Risico per locatie: hoog (>=5) vs laag (<5)
    loc_stats = []
    for loc in df["locatie"].unique():
        sub = df[df["locatie"] == loc]
        hoog = int((sub["risico"] >= RISK_HIGH_THRESHOLD).sum())
        laag = len(sub) - hoog
        loc_stats.append({"Locatie": loc, "Hoog risico": hoog, "Laag risico": laag})
    if loc_stats:
        fig_l = go.Figure()
        ls = pd.DataFrame(loc_stats)
        fig_l.add_trace(go.Bar(name="Laag risico", x=ls["Locatie"], y=ls["Laag risico"], marker_color="#48bb78"))
        fig_l.add_trace(go.Bar(name=f"Hoog risico (≥{RISK_HIGH_THRESHOLD})", x=ls["Locatie"], y=ls["Hoog risico"], marker_color="#e05a6b"))
        fig_l.update_layout(barmode="stack", margin=dict(t=40,b=0,l=0,r=0), height=220,
                             title="Risico per locatie", title_font=dict(size=13, family="Open Sans"),
                             font_family="Open Sans", legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
                             yaxis_title="Servers")
        st.plotly_chart(fig_l, use_container_width=True, key="v_loc")

# ═══════════════════════════════════════════════════════════════════════════════
#  SERVERTABEL — gesorteerd op risicoscore
# ═══════════════════════════════════════════════════════════════════════════════
tbl_l, tbl_r = st.columns([4, 1])
with tbl_l:
    st.markdown("### Servers")
with tbl_r:
    if not df.empty:
        exp_cols = ["naam","risico","status","locatie","beheerder","backup_flag","monitoring_type",
                    "min_free_pct","cpu_pct","mem_pct","os","ip_adres","functie"]
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df[[c for c in exp_cols if c in df.columns]].to_excel(w, index=False, sheet_name="Servers")
        st.download_button("📥 Excel", data=buf.getvalue(), file_name=f"PMC_{NOW.strftime('%Y%m%d')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def _s(s):  return {"poweredOn":"✅","poweredOff":"❌","suspended":"⏸"}.get(s,s)
def _m(t):  return {"SCOM":"🟣 SCOM","Nagios":"🔵 Nagios"}.get(t,"–") if t else "–"
def _d(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return "–"
    v=int(v); return f"{'🔴' if v<15 else '🟠' if v<30 else '🟢'}{v}%"
def _p(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return "–"
    v=int(v); return f"{'🔴' if v>90 else '🟠' if v>70 else '🟢'}{v}%"

def _v(row):
    if not row.get("r7_scanned"): return "–"
    c, h = int(row.get("vuln_critical", 0)), int(row.get("vuln_high", 0))
    if c > 0: return f"🔴 {c}C {h}H"
    if h > 0: return f"🟠 {h}H"
    return "🟢 OK"

df_sorted = df.sort_values("risico", ascending=False)
tbl = df_sorted[["naam","risico","risico_detail","status","locatie","beheerder","backup_flag","monitoring_type",
                  "min_free_pct","cpu_pct","mem_pct","os","functie"]].copy()
tbl["status"]          = tbl["status"].map(_s)
tbl["backup_flag"]     = tbl["backup_flag"].map({"Ja":"✅","Nee":"❌"})
tbl["monitoring_type"] = tbl["monitoring_type"].apply(_m)
tbl["min_free_pct"]    = df_sorted["min_free_pct"].apply(_d)
tbl["cpu_pct"]         = df_sorted["cpu_pct"].apply(_p)
tbl["mem_pct"]         = df_sorted["mem_pct"].apply(_p)
tbl["vulns"]           = df_sorted.apply(_v, axis=1)

tbl = tbl.rename(columns={"naam":"Server","risico":"Risico","risico_detail":"Risico details","status":"","locatie":"Locatie",
    "beheerder":"Beheerder","backup_flag":"BU","monitoring_type":"24x7","min_free_pct":"Schijf",
    "cpu_pct":"CPU","mem_pct":"RAM","os":"OS","functie":"Functie","vulns":"Vulns"})

event = st.dataframe(tbl, use_container_width=True, height=500, hide_index=True,
    on_select="rerun", selection_mode="single-row",
    column_config={
        "Server": st.column_config.TextColumn(width="medium"),
        "Risico": st.column_config.ProgressColumn("Risico", min_value=0, max_value=10, format="%d", width="small"),
        "Risico details": st.column_config.TextColumn("Details", width="large", help="Breakdown van de risicoscore per server"),
        "":       st.column_config.TextColumn(width="small"),
        "Locatie":st.column_config.TextColumn(width="medium"),
        "BU":     st.column_config.TextColumn(width="small"),
        "24x7":   st.column_config.TextColumn(width="small"),
        "Vulns":  st.column_config.TextColumn("Vulns", width="small", help="Rapid7: C=Critical H=High"),
        "Schijf": st.column_config.TextColumn(width="small"),
        "CPU":    st.column_config.TextColumn(width="small"),
        "RAM":    st.column_config.TextColumn(width="small"),
        "OS":     st.column_config.TextColumn(width="large"),
        "Functie":st.column_config.TextColumn(width="medium"),
    })

# ═══════════════════════════════════════════════════════════════════════════════
#  SQL LICENTIES (inklapbaar, onderaan)
# ═══════════════════════════════════════════════════════════════════════════════
sql_vms = df_all[df_all["sql_lic_edition"] != ""].copy()
if not sql_vms.empty:
    tc = sql_vms["vcpu"].sum()
    eds = sql_vms["sql_lic_edition"].value_counts()
    with st.expander(f"🗄️ SQL Licenties ({len(sql_vms)} servers · {tc} cores)"):
        sq1, sq2 = st.columns([2,1])
        with sq1:
            st_tbl = sql_vms[["naam","functie","sql_lic_version","sql_lic_edition","vcpu"]].rename(
                columns={"naam":"Server","functie":"Functie","sql_lic_version":"Versie","sql_lic_edition":"Editie","vcpu":"Cores"})
            st.dataframe(st_tbl, use_container_width=True, hide_index=True)
        with sq2:
            for ed, cnt in eds.items():
                c = int(sql_vms[sql_vms["sql_lic_edition"]==ed]["vcpu"].sum())
                st.write(f"**{ed}:** {cnt}x ({c} cores)")
            st.metric("Totaal", f"{tc} cores")

# ═══════════════════════════════════════════════════════════════════════════════
#  DETAILPANEEL
# ═══════════════════════════════════════════════════════════════════════════════
selected_rows = event.selection.rows if event and event.selection else []
if selected_rows:
    idx = df_sorted.index[selected_rows[0]]
    vm  = df_sorted.loc[idx]
    st.divider()
    sc = "#48bb78" if vm["status"]=="poweredOn" else "#e05a6b"
    risk_label = f'<span style="background:#fef2f2;color:#e05a6b;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700">Risico {int(vm["risico"])}</span>' if vm["risico"] > 0 else ""
    st.markdown(f"""<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
      <div style="width:10px;height:10px;border-radius:50%;background:{sc}"></div>
      <span style="font-size:20px;font-weight:800;color:#333">{vm['naam']}</span>
      <span style="font-size:11px;color:#888;background:#f5f0fa;padding:2px 10px;border-radius:12px">{vm['locatie']}</span>
      <span style="font-size:11px;color:#888;background:#f5f0fa;padding:2px 10px;border-radius:12px">{vm['beheerder'] or '–'}</span>
      {risk_label}
    </div>""", unsafe_allow_html=True)

    tab1,tab2,tab3,tab4,tab5 = st.tabs(["Overzicht","Opslag","Backup & Reboot","Tools & Monitoring","Netwerk"])
    with tab1:
        d1,d2,d3 = st.columns(3)
        with d1:
            st.write(f"**Datacenter:** {vm['datacenter']}")
            st.write(f"**Cluster:** {vm['cluster']}")
            st.write(f"**Host:** {vm['esxi_host']}")
            st.write(f"**OS:** {vm['os'] or '–'}")
            st.write(f"**Aangemaakt:** {vm['aanmaakdatum']}")
        with d2:
            if vm['cpu_pct'] is not None and not (isinstance(vm['cpu_pct'],float) and pd.isna(vm['cpu_pct'])):
                st.write(f"**CPU:** {_p(vm['cpu_pct'])} ({vm['cpu_overall_mhz']}/{vm['cpu_max_mhz']} MHz)")
            st.write(f"**vCPU:** {vm['vcpu']} · **RAM:** {vm['geheugen_gib']} GB")
            if vm['mem_pct'] is not None and not (isinstance(vm['mem_pct'],float) and pd.isna(vm['mem_pct'])):
                st.write(f"**RAM:** {_p(vm['mem_pct'])} ({vm['mem_consumed']}/{vm['mem_size']} MiB)")
                if vm['mem_swapped'] and vm['mem_swapped']>0: st.warning(f"Swapped: {vm['mem_swapped']} MiB")
                if vm['mem_ballooned'] and vm['mem_ballooned']>0: st.warning(f"Ballooned: {vm['mem_ballooned']} MiB")
        with d3:
            st.write(f"**Klantnaam:** {vm['klantnaam'] or '–'}")
            st.write(f"**Contract:** {vm['contract_nr'] or '–'}")
            st.write(f"**Functie:** {vm['functie'] or '–'}")
            if vm['sql_lic_edition']: st.write(f"**SQL:** {vm['sql_lic_version']} ({vm['sql_lic_edition']})")
            elif vm['sql_versie']: st.write(f"**SQL:** {vm['sql_versie']} ({vm['sql_editie']})")
            st.write(f"**HA:** {'✅' if vm['ha_beschermd'] else '–'}")
    with tab2:
        parts = vm['partitions'] if isinstance(vm['partitions'],list) else []
        if parts:
            for p in parts:
                cap=p['capacity_mib']; free=p['free_pct']; pct=round((1-free/100)*100) if cap else 0
                col="#48bb78" if free>=30 else ("#c49a2c" if free>=15 else "#e05a6b")
                st.markdown(f'<div style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600"><span>{p["disk"] or "?"}</span><span>{free}% vrij ({round(p["free_mib"]/1024,1)}/{round(cap/1024,1)} GB)</span></div><div style="background:#eee;border-radius:4px;height:8px;overflow:hidden"><div style="background:{col};height:100%;width:{pct}%;border-radius:4px"></div></div></div>', unsafe_allow_html=True)
        else: st.caption("Geen partitie-data")
    with tab3:
        b1,b2 = st.columns(2)
        with b1:
            st.write(f"**Backup:** {'✅' if vm['backup_flag']=='Ja' else '❌'}")
            bds = fmt_date(vm['backup_datum'])
            if bds:
                d=vm['dagen_backup']
                st.write(f"**Laatste:** {'🟢' if d<=BACKUP_WARN_DAYS else '🔴'} {bds} ({d}d)")
            if vm['backup_str']: st.code(vm['backup_str'], language=None)
        with b2:
            rs=fmt_date(vm['laatste_reboot'])
            if rs:
                d=vm['dagen_reboot'] or 0
                st.write(f"**Reboot:** {'🟢' if d<=REBOOT_WARN_DAYS else '🟠'} {rs} ({d}d)")
            else: st.write("**Reboot:** –")
            hs=fmt_date(vm['host_boot'])
            if hs:
                d=vm['dagen_host_boot'] or 0
                st.write(f"**Host boot:** {'🟢' if d<=REBOOT_WARN_DAYS else '🟠'} {hs} ({d}d)")
            st.write(f"**Patch schema:** {vm['update_moment'] or '–'}")
    with tab4:
        t1,t2 = st.columns(2)
        with t1:
            st.write(f"**Tools:** {TOOLS_LABELS.get(vm['tools_status'],vm['tools_status'])}")
            st.write(f"**Versie:** {vm['tools_versie'] or '–'} · **Upgrade:** {vm['tools_upgradeable'] or '–'}")
            if vm['heeft_snapshot']: st.warning("Actieve snapshot")
        with t2:
            st.write(f"**24x7:** {_m(vm['monitoring_type'])}")
            if vm['monitoring_functie']: st.write(f"**Registratie:** {vm['monitoring_functie']}")
            health = vm['health_messages'] if isinstance(vm['health_messages'],list) else []
            for msg in health: st.warning(msg)
    with tab5:
        nics = vm['nics'] if isinstance(vm['nics'],list) else []
        if nics:
            nd = pd.DataFrame(nics); nd.columns=["NIC","VLAN","Switch","Conn","MAC","IPv4"]
            nd["Conn"]=nd["Conn"].map({True:"✅",False:"❌"})
            st.dataframe(nd, use_container_width=True, hide_index=True)
        else: st.caption("Geen netwerkdata")
        st.write(f"**IP:** {vm['ip_adres'] or '–'}")
        if vm['wiki_link']: st.markdown(f"[Wiki]({vm['wiki_link']})")
else:
    st.caption("Klik op een server voor details")

# ─── Footer ──────────────────────────────────────────────────────────────────
st.divider()
st.caption(f"ram infotechnology · Prinses Maxima Centrum · {NOW.strftime('%d-%m-%Y')} · vCenter + SCOM/Nagios + SQL licenties")
