"""
Prinses Maxima Server Dashboard Generator
Leest vCenter Excel-exports en genereert een zelfstandig HTML-dashboard.
Gebruik: python3 generate_dashboard.py
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("pandas niet gevonden. Installeer met: pip install pandas openpyxl")
    sys.exit(1)

# ─── Configuratie ──────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
FILE1      = BASE_DIR / "vALL-pmc-vCenter.xlsx"
FILE2      = BASE_DIR / "Vcenter overzicht Prinses Maxima PPD - GK d.d. 01-04-2026 v1.xlsx"
OUTPUT     = Path(__file__).parent / "dashboard.html"
NOW        = datetime.now()
NOW_STR    = NOW.strftime("%d-%m-%Y %H:%M")
BACKUP_WARN_DAYS = 2
REBOOT_WARN_DAYS = 90

# ─── Data laden ────────────────────────────────────────────────────────────────

def parse_backup_date(text):
    """Parseer 'Last backup: [DD-M-YYYY HH:MM:SS]' uit Veeam annotatie string."""
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
    """Parseer kernelVersion uit Guest Detailed Data string."""
    if not text or not isinstance(text, str):
        return None
    m = re.search(r"kernelVersion='([^']+)'", text)
    return m.group(1) if m else None


def normalize_backup_flag(val):
    if not val or not isinstance(val, str):
        return "Nee"
    return "Ja" if val.strip().lower() in ("ja", "yes", "true", "1") else "Nee"


def days_since(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None
    try:
        return (NOW - dt.replace(tzinfo=None)).days
    except Exception:
        return None


def load_file1():
    print("Bestand 1 laden: vALL-pmc-vCenter.xlsx ...")

    # vInfo
    df = pd.read_excel(FILE1, sheet_name="vInfo", engine="openpyxl")
    # Filter templates (geen echte VMs)
    df = df[df["Template"] != True].copy()

    # vTools → dict
    df_tools = pd.read_excel(FILE1, sheet_name="vTools", engine="openpyxl")
    tools_map = {}
    for _, r in df_tools.iterrows():
        tools_map[r["VM"]] = {
            "tools_status":      r.get("Tools", ""),
            "tools_versie":      r.get("Tools Version", ""),
            "tools_vereist":     r.get("Required Version", ""),
            "tools_upgradeable": r.get("Upgradeable", ""),
        }

    # vHost → dict (boot time per host)
    df_host = pd.read_excel(FILE1, sheet_name="vHost", engine="openpyxl")
    host_map = {}
    for _, r in df_host.iterrows():
        host_map[r["Host"]] = {
            "host_boot_tijd": r.get("Boot time", None),
            "esx_versie":     r.get("ESX Version", ""),
        }

    # vSnapshot → set van VM-namen met actieve (niet-Veeam) snapshots
    df_snap = pd.read_excel(FILE1, sheet_name="vSnapshot", engine="openpyxl")
    snapshot_vms = set(
        df_snap[df_snap["Name"] != "VEEAM BACKUP TEMPORARY SNAPSHOT"]["VM"].tolist()
    )

    # Bouw unified DataFrame
    records = []
    for _, r in df.iterrows():
        vm_name = r["VM"]
        tools   = tools_map.get(vm_name, {})
        host    = host_map.get(r.get("Host", ""), {})

        # Backup datum: uit Annotation
        backup_str       = str(r.get("Annotation", "") or "")
        backup_datum     = parse_backup_date(backup_str)
        backup_flag      = normalize_backup_flag(str(r.get("Backup", "") or ""))

        # Als Backup = Nee maar er WEL een backup datum is → corrigeer
        if backup_datum and backup_flag == "Nee":
            backup_flag = "Ja"

        reboot_dt = r.get("PowerOn", None)
        try:
            reboot_dt = reboot_dt.to_pydatetime() if hasattr(reboot_dt, 'to_pydatetime') else reboot_dt
            if pd.isna(reboot_dt): reboot_dt = None
        except Exception:
            reboot_dt = None

        host_boot = host.get("host_boot_tijd", None)
        try:
            host_boot = host_boot.to_pydatetime() if hasattr(host_boot, 'to_pydatetime') else host_boot
            if pd.isna(host_boot): host_boot = None
        except Exception:
            host_boot = None

        records.append({
            "naam":               vm_name,
            "status":             r.get("Powerstate", ""),
            "locatie":            "PMC On-Premises",
            "datacenter":         str(r.get("Datacenter", "") or ""),
            "cluster":            str(r.get("Cluster", "") or ""),
            "esxi_host":          str(r.get("Host", "") or ""),
            "ip_adres":           str(r.get("Primary IP Address", "") or ""),
            "vcpu":               r.get("CPUs", ""),
            "geheugen_gib":       round(float(r.get("Memory", 0) or 0) / 1024, 1),
            "os":                 str(r.get("OS according to the VMware Tools", "") or ""),
            "os_config":          str(r.get("OS according to the configuration file", "") or ""),
            "kernel_versie":      parse_kernel_version(str(r.get("Guest Detailed Data", "") or "")),
            "beheerder":          str(r.get("Beheerder", "") or ""),
            "afdeling":           str(r.get("Afdeling", "") or ""),
            "functie":            str(r.get("FunctieServer", "") or ""),
            "soort":              str(r.get("SoortServer", "") or ""),
            "sql_versie":         str(r.get("SQLVersion", "") or ""),
            "sql_editie":         str(r.get("SQLEdition", "") or ""),
            "update_schema":      str(r.get("Update", "") or ""),
            "update_moment":      str(r.get("Updatemoment", "") or ""),
            "klantnaam":          str(r.get("Klantnaam", "") or ""),
            "contract_nr":        str(r.get("AFASContractnummer", "") or ""),
            "ha_beschermd":       bool(r.get("DAS protection", False)),
            "aanmaakdatum":       str(r.get("Creation date", "") or ""),
            "wiki_link":          "",
            "backup_flag":        backup_flag,
            "backup_str":         backup_str[:120] if backup_str else "",
            "backup_datum":       backup_datum.strftime("%d-%m-%Y %H:%M") if backup_datum else "",
            "dagen_backup":       days_since(backup_datum),
            "laatste_reboot":     reboot_dt.strftime("%d-%m-%Y %H:%M") if reboot_dt else "",
            "dagen_reboot":       days_since(reboot_dt),
            "host_boot":          host_boot.strftime("%d-%m-%Y %H:%M") if host_boot else "",
            "dagen_host_boot":    days_since(host_boot),
            "esx_versie":         host.get("esx_versie", ""),
            "tools_status":       tools.get("tools_status", ""),
            "tools_versie":       str(tools.get("tools_versie", "") or ""),
            "tools_upgradeable":  str(tools.get("tools_upgradeable", "") or ""),
            "heeft_snapshot":     vm_name in snapshot_vms,
            "bron":               "PMC",
        })

    return pd.DataFrame(records)


def load_file2():
    print("Bestand 2 laden: Vcenter overzicht PPD - GK ...")
    df = pd.read_excel(FILE2, sheet_name="Blad1", engine="openpyxl")
    df = df[df["Template"] != True].copy()

    records = []
    for _, r in df.iterrows():
        backup_str   = str(r.get("backup", "") or "") + " " + str(r.get("backup_details", "") or "")
        backup_str   = backup_str.strip()
        backup_datum = parse_backup_date(backup_str)
        backup_flag  = "Ja" if backup_datum else "Nee"

        reboot_dt = r.get("PowerOn", None)
        try:
            reboot_dt = reboot_dt.to_pydatetime() if hasattr(reboot_dt, 'to_pydatetime') else reboot_dt
            if pd.isna(reboot_dt): reboot_dt = None
        except Exception:
            reboot_dt = None

        # Locatie: afleiden uit Datacenter kolom
        dc = str(r.get("Datacenter", "") or "")
        if "papend" in dc.lower() or "ppd" in dc.lower():
            locatie = "RAM DC – Papendorp"
        elif "groen" in dc.lower() or "gk" in dc.lower():
            locatie = "RAM DC – Groenekan"
        else:
            locatie = f"RAM DC – {dc}" if dc else "RAM DC"

        records.append({
            "naam":               r.get("VM", ""),
            "status":             r.get("Powerstate", ""),
            "locatie":            locatie,
            "datacenter":         dc,
            "cluster":            str(r.get("Cluster", "") or ""),
            "esxi_host":          str(r.get("Host", "") or ""),
            "ip_adres":           str(r.get("Primary IP Address", "") or ""),
            "vcpu":               r.get("CPUs", ""),
            "geheugen_gib":       round(float(r.get("Memory", 0) or 0) / 1024, 1),
            "os":                 str(r.get("OS according to the VMware Tools", "") or ""),
            "os_config":          str(r.get("OS according to the configuration file", "") or ""),
            "kernel_versie":      parse_kernel_version(str(r.get("Guest Detailed Data", "") or "")),
            "beheerder":          str(r.get("Beheerder", "") or ""),
            "afdeling":           str(r.get("Afdeling", "") or ""),
            "functie":            str(r.get("FunctieServer", "") or ""),
            "soort":              str(r.get("SoortServer", "") or ""),
            "sql_versie":         str(r.get("SQLVersion", "") or ""),
            "sql_editie":         str(r.get("SQLEdition", "") or ""),
            "update_schema":      str(r.get("Update", "") or ""),
            "update_moment":      str(r.get("Updatemoment", "") or ""),
            "klantnaam":          str(r.get("Klantnaam", "") or ""),
            "contract_nr":        str(r.get("AFASContractnummer", "") or ""),
            "ha_beschermd":       bool(r.get("DAS protection", False)),
            "aanmaakdatum":       str(r.get("Creation date", "") or ""),
            "wiki_link":          str(r.get("WikiLink", "") or ""),
            "backup_flag":        backup_flag,
            "backup_str":         backup_str[:120],
            "backup_datum":       backup_datum.strftime("%d-%m-%Y %H:%M") if backup_datum else "",
            "dagen_backup":       days_since(backup_datum),
            "laatste_reboot":     reboot_dt.strftime("%d-%m-%Y %H:%M") if reboot_dt else "",
            "dagen_reboot":       days_since(reboot_dt),
            "host_boot":          "",
            "dagen_host_boot":    None,
            "esx_versie":         "",
            "tools_status":       "toolsOnbekend",
            "tools_versie":       "",
            "tools_upgradeable":  "",
            "heeft_snapshot":     False,
            "bron":               "RAM-DC",
        })

    return pd.DataFrame(records)


def compute_kpis(df):
    total    = len(df)
    aan      = (df["status"] == "poweredOn").sum()
    uit      = (df["status"] == "poweredOff").sum()
    backup_ja = (df["backup_flag"] == "Ja").sum()
    backup_pct = round(backup_ja / total * 100) if total else 0
    tools_issue = ((df["tools_status"] != "toolsOk") & (df["tools_status"] != "toolsOnbekend")).sum()
    return {
        "total": int(total),
        "aan": int(aan),
        "uit": int(uit),
        "suspended": int(total - aan - uit),
        "backup_ja": int(backup_ja),
        "backup_pct": int(backup_pct),
        "tools_issue": int(tools_issue),
    }


def compute_chart_data(df):
    # Status per locatie
    status_labels = ["Aan (poweredOn)", "Uit (poweredOff)", "Gesuspendeerd"]
    status_data = [
        int((df["status"] == "poweredOn").sum()),
        int((df["status"] == "poweredOff").sum()),
        int((df["status"] == "suspended").sum()),
    ]

    # Backup
    backup_labels = ["Backup geconfigureerd", "Geen backup"]
    backup_data = [
        int((df["backup_flag"] == "Ja").sum()),
        int((df["backup_flag"] == "Nee").sum()),
    ]

    # Tools status
    tools_counts = df["tools_status"].value_counts().to_dict()
    tools_labels = []
    tools_data   = []
    tools_map_labels = {
        "toolsOk":            "OK",
        "toolsOld":           "Verouderd",
        "toolsNotRunning":    "Niet actief",
        "toolsNotInstalled":  "Niet geinstalleerd",
        "toolsOnbekend":      "Onbekend",
    }
    for key, label in tools_map_labels.items():
        count = tools_counts.get(key, 0)
        if count > 0:
            tools_labels.append(label)
            tools_data.append(int(count))

    # Locatie verdeling
    loc_counts = df["locatie"].value_counts().to_dict()
    loc_labels = list(loc_counts.keys())
    loc_data   = [int(v) for v in loc_counts.values()]

    return {
        "status": {"labels": status_labels, "data": status_data},
        "backup": {"labels": backup_labels, "data": backup_data},
        "tools":  {"labels": tools_labels,  "data": tools_data},
        "locatie": {"labels": loc_labels,   "data": loc_data},
    }


def compute_alerts(df):
    alerts = []

    no_backup = df[df["backup_flag"] == "Nee"]
    if not no_backup.empty:
        alerts.append({
            "type": "danger",
            "icon": "⚠",
            "title": f"{len(no_backup)} VM's zonder backup geconfigureerd",
            "items": no_backup["naam"].tolist(),
        })

    stale_backup = df[(df["backup_flag"] == "Ja") & (df["dagen_backup"].notna()) & (df["dagen_backup"] > BACKUP_WARN_DAYS)]
    if not stale_backup.empty:
        alerts.append({
            "type": "warning",
            "icon": "⏰",
            "title": f"{len(stale_backup)} VM's met laatste backup ouder dan {BACKUP_WARN_DAYS} dagen",
            "items": [f"{r['naam']} ({r['backup_datum']})" for _, r in stale_backup.iterrows()],
        })

    tools_old = df[df["tools_status"].isin(["toolsOld", "toolsNotRunning", "toolsNotInstalled"])]
    if not tools_old.empty:
        label_map = {"toolsOld": "Verouderd", "toolsNotRunning": "Niet actief", "toolsNotInstalled": "Niet geinstalleerd"}
        alerts.append({
            "type": "warning",
            "icon": "🔧",
            "title": f"{len(tools_old)} VM's met VMware Tools aandacht nodig",
            "items": [f"{r['naam']} ({label_map.get(r['tools_status'], r['tools_status'])})" for _, r in tools_old.iterrows()],
        })

    snaps = df[df["heeft_snapshot"] == True]
    if not snaps.empty:
        alerts.append({
            "type": "info",
            "icon": "📸",
            "title": f"{len(snaps)} VM's met actieve snapshot",
            "items": snaps["naam"].tolist(),
        })

    off_with_backup = df[(df["status"] == "poweredOff") & (df["backup_flag"] == "Ja")]
    if not off_with_backup.empty:
        alerts.append({
            "type": "info",
            "icon": "⚡",
            "title": f"{len(off_with_backup)} VM's uitgeschakeld maar backup geconfigureerd",
            "items": off_with_backup["naam"].tolist(),
        })

    old_reboot = df[(df["dagen_reboot"].notna()) & (df["dagen_reboot"] > REBOOT_WARN_DAYS)]
    if not old_reboot.empty:
        alerts.append({
            "type": "info",
            "icon": "🔄",
            "title": f"{len(old_reboot)} VM's niet herstart in meer dan {REBOOT_WARN_DAYS} dagen",
            "items": [f"{r['naam']} ({r['laatste_reboot']})" for _, r in old_reboot.iterrows()],
        })

    return alerts


# ─── HTML generatie ────────────────────────────────────────────────────────────

def df_to_json(df):
    """Converteer DataFrame naar lijst van dicts, JSON-serialiseerbaar."""
    records = []
    for _, r in df.iterrows():
        record = {}
        for col in df.columns:
            val = r[col]
            if pd.isna(val) if not isinstance(val, (bool, str, list)) else False:
                val = None
            elif isinstance(val, bool):
                pass
            elif hasattr(val, 'item'):
                val = val.item()
            record[col] = val
        records.append(record)
    return records


def render_html(df, kpis, chart_data, alerts):
    vm_data_json    = json.dumps(df_to_json(df), ensure_ascii=False, default=str)
    kpis_json       = json.dumps(kpis)
    chart_data_json = json.dumps(chart_data, ensure_ascii=False)
    alerts_json     = json.dumps(alerts, ensure_ascii=False)

    # Unieke beheerders en soorten voor filter dropdowns
    beheerders = sorted(df["beheerder"].dropna().unique().tolist())
    soorten    = sorted([s for s in df["soort"].dropna().unique().tolist() if s])
    locaties   = sorted(df["locatie"].dropna().unique().tolist())

    beheerder_options = "\n".join(f'<option value="{b}">{b}</option>' for b in beheerders if b)
    soort_options     = "\n".join(f'<option value="{s}">{s}</option>' for s in soorten if s)
    locatie_options   = "\n".join(f'<option value="{l}">{l}</option>' for l in locaties if l)

    html = f"""<!DOCTYPE html>
<html lang="nl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Server Overzicht – Prinses Maxima Centrum</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f4f8; color: #1a202c; font-size: 14px; }}

  /* Header */
  .header {{ background: linear-gradient(135deg, #1565c0 0%, #0d47a1 100%); color: white; padding: 20px 32px; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 2px 8px rgba(0,0,0,0.2); }}
  .header-left h1 {{ font-size: 20px; font-weight: 700; letter-spacing: -0.3px; }}
  .header-left p {{ font-size: 12px; opacity: 0.8; margin-top: 2px; }}
  .header-right {{ text-align: right; font-size: 11px; opacity: 0.75; }}
  .logo {{ width: 36px; height: 36px; background: white; border-radius: 8px; display: inline-flex; align-items: center; justify-content: center; color: #1565c0; font-weight: 900; font-size: 14px; margin-right: 12px; flex-shrink: 0; }}

  /* Main layout */
  .main {{ padding: 24px 32px; max-width: 1600px; margin: 0 auto; }}

  /* KPI Cards */
  .kpi-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 24px; }}
  .kpi-card {{ background: white; border-radius: 12px; padding: 16px 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-left: 4px solid #e2e8f0; }}
  .kpi-card.blue {{ border-left-color: #1565c0; }}
  .kpi-card.green {{ border-left-color: #2e7d32; }}
  .kpi-card.orange {{ border-left-color: #ef6c00; }}
  .kpi-card.teal {{ border-left-color: #00838f; }}
  .kpi-card.red {{ border-left-color: #c62828; }}
  .kpi-label {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #718096; font-weight: 600; }}
  .kpi-value {{ font-size: 28px; font-weight: 800; margin-top: 4px; color: #1a202c; }}
  .kpi-sub {{ font-size: 11px; color: #a0aec0; margin-top: 2px; }}

  /* Charts */
  .charts-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px; }}
  .chart-card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .chart-card h3 {{ font-size: 13px; font-weight: 600; color: #4a5568; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.4px; }}
  .chart-wrap {{ height: 180px; display: flex; align-items: center; justify-content: center; }}

  /* Alerts */
  .alerts-panel {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 24px; }}
  .alerts-header {{ display: flex; align-items: center; justify-content: space-between; cursor: pointer; user-select: none; }}
  .alerts-header h2 {{ font-size: 14px; font-weight: 700; color: #1a202c; }}
  .alerts-header .badge {{ background: #fed7d7; color: #c62828; border-radius: 20px; padding: 2px 10px; font-size: 11px; font-weight: 700; }}
  .alerts-body {{ margin-top: 16px; display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }}
  .alert-item {{ border-radius: 8px; padding: 12px 14px; font-size: 12px; }}
  .alert-item.danger {{ background: #fff5f5; border: 1px solid #feb2b2; }}
  .alert-item.warning {{ background: #fffbeb; border: 1px solid #fbd38d; }}
  .alert-item.info {{ background: #ebf8ff; border: 1px solid #90cdf4; }}
  .alert-item .alert-title {{ font-weight: 700; margin-bottom: 6px; font-size: 12px; }}
  .alert-item.danger .alert-title {{ color: #c62828; }}
  .alert-item.warning .alert-title {{ color: #c05621; }}
  .alert-item.info .alert-title {{ color: #2b6cb0; }}
  .alert-item ul {{ list-style: none; padding: 0; }}
  .alert-item ul li {{ color: #4a5568; padding: 1px 0; }}
  .alert-item ul li::before {{ content: "• "; opacity: 0.5; }}
  .alert-collapsed {{ display: none; }}

  /* Filter bar */
  .filter-bar {{ background: white; border-radius: 12px; padding: 16px 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 16px; display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
  .filter-bar label {{ font-size: 11px; font-weight: 600; color: #718096; text-transform: uppercase; letter-spacing: 0.4px; display: block; margin-bottom: 3px; }}
  .filter-group {{ display: flex; flex-direction: column; }}
  .filter-bar input, .filter-bar select {{ border: 1px solid #e2e8f0; border-radius: 6px; padding: 6px 10px; font-size: 13px; color: #1a202c; background: white; outline: none; }}
  .filter-bar input {{ min-width: 200px; }}
  .filter-bar input:focus, .filter-bar select:focus {{ border-color: #1565c0; box-shadow: 0 0 0 2px rgba(21,101,192,0.15); }}
  .btn-reset {{ background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 7px 14px; font-size: 12px; color: #4a5568; cursor: pointer; font-weight: 600; margin-top: 18px; }}
  .btn-reset:hover {{ background: #edf2f7; }}

  /* Table */
  .table-card {{ background: white; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); overflow: hidden; }}
  .table-header {{ padding: 16px 20px; border-bottom: 1px solid #e2e8f0; display: flex; align-items: center; justify-content: space-between; }}
  .table-header h2 {{ font-size: 14px; font-weight: 700; color: #1a202c; }}
  .table-count {{ font-size: 12px; color: #a0aec0; }}
  .table-wrap {{ overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; }}
  th {{ background: #f7fafc; padding: 10px 12px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.4px; color: #718096; font-weight: 700; border-bottom: 2px solid #e2e8f0; white-space: nowrap; cursor: pointer; user-select: none; }}
  th:hover {{ background: #edf2f7; }}
  th.sort-asc::after {{ content: " ↑"; color: #1565c0; }}
  th.sort-desc::after {{ content: " ↓"; color: #1565c0; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #f0f4f8; vertical-align: middle; }}
  tr:hover td {{ background: #f7fafc; }}
  tr.row-alert td {{ background: #fff5f5; }}
  tr.row-alert:hover td {{ background: #fed7d7; }}
  tr.row-warn td {{ background: #fffbeb; }}
  tr.row-warn:hover td {{ background: #fefcbf; }}

  /* Badges */
  .badge-aan {{ background: #c6f6d5; color: #276749; border-radius: 20px; padding: 2px 9px; font-weight: 700; font-size: 11px; white-space: nowrap; }}
  .badge-uit {{ background: #fed7d7; color: #9b2c2c; border-radius: 20px; padding: 2px 9px; font-weight: 700; font-size: 11px; white-space: nowrap; }}
  .badge-susp {{ background: #fefcbf; color: #744210; border-radius: 20px; padding: 2px 9px; font-weight: 700; font-size: 11px; white-space: nowrap; }}
  .badge-ok {{ background: #c6f6d5; color: #276749; border-radius: 20px; padding: 2px 9px; font-weight: 600; font-size: 11px; }}
  .badge-warn {{ background: #fefcbf; color: #744210; border-radius: 20px; padding: 2px 9px; font-weight: 600; font-size: 11px; }}
  .badge-err {{ background: #fed7d7; color: #9b2c2c; border-radius: 20px; padding: 2px 9px; font-weight: 600; font-size: 11px; }}
  .badge-grey {{ background: #e2e8f0; color: #4a5568; border-radius: 20px; padding: 2px 9px; font-weight: 600; font-size: 11px; }}
  .badge-ha {{ background: #bee3f8; color: #2c5282; border-radius: 20px; padding: 2px 9px; font-weight: 600; font-size: 11px; }}

  .os-cell {{ max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .date-warn {{ color: #c05621; font-weight: 600; }}
  .date-ok {{ color: #276749; }}
  .wiki-link {{ color: #1565c0; text-decoration: none; }}
  .wiki-link:hover {{ text-decoration: underline; }}

  /* Pagination */
  .pagination {{ display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; border-top: 1px solid #e2e8f0; }}
  .pagination-info {{ font-size: 12px; color: #a0aec0; }}
  .pagination-btns {{ display: flex; gap: 6px; }}
  .page-btn {{ border: 1px solid #e2e8f0; background: white; border-radius: 6px; padding: 4px 10px; font-size: 12px; cursor: pointer; color: #4a5568; }}
  .page-btn:hover {{ background: #edf2f7; }}
  .page-btn.active {{ background: #1565c0; color: white; border-color: #1565c0; }}
  .page-btn:disabled {{ opacity: 0.4; cursor: default; }}

  @media print {{
    body {{ background: white; }}
    .filter-bar, .btn-reset, .pagination {{ display: none; }}
    .main {{ padding: 0; max-width: 100%; }}
    .table-wrap {{ overflow: visible; }}
  }}
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center">
    <div class="logo">RAM</div>
    <div class="header-left">
      <h1>Server Overzicht – Prinses Maxima Centrum</h1>
      <p>VMware vCenter Infrastructure Rapportage</p>
    </div>
  </div>
  <div class="header-right">
    Gegenereerd op: <strong>{NOW_STR}</strong><br>
    Brondata: vALL-pmc-vCenter.xlsx + Vcenter overzicht PPD-GK
  </div>
</div>

<div class="main">

  <!-- KPI Cards -->
  <div class="kpi-grid" id="kpi-grid"></div>

  <!-- Charts -->
  <div class="charts-grid">
    <div class="chart-card">
      <h3>Server Status</h3>
      <div class="chart-wrap"><canvas id="chart-status"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Backup Status</h3>
      <div class="chart-wrap"><canvas id="chart-backup"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>VMware Tools Status</h3>
      <div class="chart-wrap"><canvas id="chart-tools"></canvas></div>
    </div>
  </div>

  <!-- Aandachtspunten -->
  <div class="alerts-panel">
    <div class="alerts-header" onclick="toggleAlerts()">
      <h2>⚠ Aandachtspunten <span class="badge" id="alert-count"></span></h2>
      <span id="alert-toggle" style="font-size:11px;color:#718096;">▼ Toon</span>
    </div>
    <div class="alerts-body alert-collapsed" id="alerts-body"></div>
  </div>

  <!-- Filter bar -->
  <div class="filter-bar">
    <div class="filter-group">
      <label>Zoeken</label>
      <input type="text" id="filter-search" placeholder="VM-naam, IP, OS..." oninput="applyFilters()">
    </div>
    <div class="filter-group">
      <label>Locatie</label>
      <select id="filter-locatie" onchange="applyFilters()">
        <option value="">Alle locaties</option>
        {locatie_options}
      </select>
    </div>
    <div class="filter-group">
      <label>Status</label>
      <select id="filter-status" onchange="applyFilters()">
        <option value="">Alle</option>
        <option value="poweredOn">Aan</option>
        <option value="poweredOff">Uit</option>
        <option value="suspended">Gesuspendeerd</option>
      </select>
    </div>
    <div class="filter-group">
      <label>Beheerder</label>
      <select id="filter-beheerder" onchange="applyFilters()">
        <option value="">Alle</option>
        {beheerder_options}
      </select>
    </div>
    <div class="filter-group">
      <label>Backup</label>
      <select id="filter-backup" onchange="applyFilters()">
        <option value="">Alle</option>
        <option value="Ja">Geconfigureerd</option>
        <option value="Nee">Niet geconfigureerd</option>
      </select>
    </div>
    <div class="filter-group">
      <label>Soort Server</label>
      <select id="filter-soort" onchange="applyFilters()">
        <option value="">Alle</option>
        {soort_options}
      </select>
    </div>
    <button class="btn-reset" onclick="resetFilters()">↺ Reset filters</button>
  </div>

  <!-- Table -->
  <div class="table-card">
    <div class="table-header">
      <h2>Servers</h2>
      <span class="table-count" id="table-count"></span>
    </div>
    <div class="table-wrap">
      <table id="vm-table">
        <thead>
          <tr>
            <th onclick="sortTable('naam')">Naam</th>
            <th onclick="sortTable('status')">Status</th>
            <th onclick="sortTable('locatie')">Locatie</th>
            <th onclick="sortTable('beheerder')">Beheerder</th>
            <th onclick="sortTable('os')">Besturingssysteem</th>
            <th onclick="sortTable('tools_status')">VMware Tools</th>
            <th onclick="sortTable('update_schema')">Patch Schema</th>
            <th onclick="sortTable('laatste_reboot')">Laatste Reboot</th>
            <th onclick="sortTable('host_boot')">Host Boot</th>
            <th onclick="sortTable('backup_flag')">Backup</th>
            <th onclick="sortTable('backup_datum')">Laatste Backup</th>
            <th onclick="sortTable('ip_adres')">IP Adres</th>
            <th onclick="sortTable('vcpu')">vCPU / RAM</th>
            <th onclick="sortTable('sql_versie')">SQL</th>
            <th onclick="sortTable('ha_beschermd')">HA</th>
            <th onclick="sortTable('functie')">Functie</th>
            <th>Wiki</th>
          </tr>
        </thead>
        <tbody id="vm-tbody"></tbody>
      </table>
    </div>
    <div class="pagination">
      <div class="pagination-info" id="page-info"></div>
      <div class="pagination-btns" id="page-btns"></div>
    </div>
  </div>

</div>

<!-- Inline Chart.js via CDN fallback script -->
<script>
// ─── Data ──────────────────────────────────────────────────────────────────────
const VM_DATA     = {vm_data_json};
const KPIS        = {kpis_json};
const CHART_DATA  = {chart_data_json};
const ALERTS      = {alerts_json};

// ─── State ─────────────────────────────────────────────────────────────────────
let filteredData  = [...VM_DATA];
let currentPage   = 1;
const PAGE_SIZE   = 25;
let sortCol       = 'naam';
let sortDir       = 'asc';

// ─── KPI Cards ─────────────────────────────────────────────────────────────────
function renderKPIs() {{
  const grid = document.getElementById('kpi-grid');
  const cards = [
    {{ label: 'Totaal Servers', value: KPIS.total, sub: 'VMs in beheer', cls: 'blue' }},
    {{ label: 'Aan', value: KPIS.aan, sub: 'poweredOn', cls: 'green' }},
    {{ label: 'Uit', value: KPIS.uit, sub: 'poweredOff', cls: 'orange' }},
    {{ label: 'Backup OK', value: KPIS.backup_pct + '%', sub: KPIS.backup_ja + ' van ' + KPIS.total + ' geconfigureerd', cls: KPIS.backup_pct === 100 ? 'green' : KPIS.backup_pct > 90 ? 'teal' : 'red' }},
    {{ label: 'Tools Aandacht', value: KPIS.tools_issue, sub: 'VMware Tools niet OK (PMC)', cls: KPIS.tools_issue === 0 ? 'green' : 'red' }},
  ];
  grid.innerHTML = cards.map(c => `
    <div class="kpi-card ${{c.cls}}">
      <div class="kpi-label">${{c.label}}</div>
      <div class="kpi-value">${{c.value}}</div>
      <div class="kpi-sub">${{c.sub}}</div>
    </div>`).join('');
}}

// ─── Badges ────────────────────────────────────────────────────────────────────
function statusBadge(s) {{
  if (s === 'poweredOn')  return '<span class="badge-aan">Aan</span>';
  if (s === 'poweredOff') return '<span class="badge-uit">Uit</span>';
  if (s === 'suspended')  return '<span class="badge-susp">Gesuspendeerd</span>';
  return s || '';
}}

function toolsBadge(s) {{
  const map = {{
    toolsOk:           ['badge-ok',   'OK'],
    toolsOld:          ['badge-warn', 'Verouderd'],
    toolsNotRunning:   ['badge-err',  'Niet actief'],
    toolsNotInstalled: ['badge-err',  'Niet geïnst.'],
    toolsOnbekend:     ['badge-grey', 'Onbekend'],
  }};
  const [cls, label] = map[s] || ['badge-grey', s || '–'];
  return `<span class="${{cls}}">${{label}}</span>`;
}}

function backupBadge(f) {{
  return f === 'Ja'
    ? '<span class="badge-ok">Ja</span>'
    : '<span class="badge-err">Nee</span>';
}}

function haBadge(v) {{
  return v ? '<span class="badge-ha">HA</span>' : '<span class="badge-grey">–</span>';
}}

function dateCell(dateStr, days, warnDays) {{
  if (!dateStr) return '<span style="color:#a0aec0">–</span>';
  if (days !== null && days !== undefined && days > warnDays)
    return `<span class="date-warn" title="${{days}} dagen geleden">${{dateStr}}</span>`;
  return `<span class="date-ok">${{dateStr}}</span>`;
}}

// ─── Tabel rendering ───────────────────────────────────────────────────────────
function renderTable() {{
  const start = (currentPage - 1) * PAGE_SIZE;
  const page  = filteredData.slice(start, start + PAGE_SIZE);
  const tbody = document.getElementById('vm-tbody');

  tbody.innerHTML = page.map(vm => {{
    const hasAlert = vm.backup_flag === 'Nee' || ['toolsOld','toolsNotRunning','toolsNotInstalled'].includes(vm.tools_status);
    const hasWarn  = vm.dagen_backup > 2 || vm.status === 'poweredOff';
    const rowCls   = hasAlert ? 'row-alert' : (hasWarn ? 'row-warn' : '');
    const sql      = vm.sql_versie && vm.sql_versie !== 'None' && vm.sql_versie !== '' ? `<span title="${{vm.sql_editie}}">${{vm.sql_versie}}</span>` : '<span style="color:#a0aec0">–</span>';
    const wiki     = vm.wiki_link ? `<a class="wiki-link" href="${{vm.wiki_link}}" target="_blank">↗</a>` : '';
    const patch    = vm.update_moment && vm.update_moment !== 'None' ? `<span style="color:#4a5568">${{vm.update_moment}}</span>` : '<span style="color:#a0aec0">–</span>';
    return `<tr class="${{rowCls}}">
      <td><strong>${{vm.naam}}</strong></td>
      <td>${{statusBadge(vm.status)}}</td>
      <td>${{vm.locatie || ''}}</td>
      <td>${{vm.beheerder || ''}}</td>
      <td class="os-cell" title="${{vm.os}}">${{vm.os || ''}}</td>
      <td>${{toolsBadge(vm.tools_status)}}</td>
      <td>${{patch}}</td>
      <td>${{dateCell(vm.laatste_reboot, vm.dagen_reboot, 90)}}</td>
      <td>${{dateCell(vm.host_boot, vm.dagen_host_boot, 90)}}</td>
      <td>${{backupBadge(vm.backup_flag)}}</td>
      <td>${{dateCell(vm.backup_datum, vm.dagen_backup, 2)}}</td>
      <td>${{vm.ip_adres || ''}}</td>
      <td>${{vm.vcpu || ''}} / ${{vm.geheugen_gib}} GB</td>
      <td>${{sql}}</td>
      <td>${{haBadge(vm.ha_beschermd)}}</td>
      <td title="${{vm.functie}}">${{(vm.functie || '').substring(0, 20)}}${{vm.functie && vm.functie.length > 20 ? '…' : ''}}</td>
      <td>${{wiki}}</td>
    </tr>`;
  }}).join('');

  const total = filteredData.length;
  document.getElementById('table-count').textContent = `${{total}} server${{total !== 1 ? 's' : ''}} ${{total < VM_DATA.length ? '(gefilterd van ' + VM_DATA.length + ')' : ''}}`;
  renderPagination(total);
}}

// ─── Pagination ────────────────────────────────────────────────────────────────
function renderPagination(total) {{
  const pages = Math.ceil(total / PAGE_SIZE);
  const info  = document.getElementById('page-info');
  const btns  = document.getElementById('page-btns');
  const start = (currentPage - 1) * PAGE_SIZE + 1;
  const end   = Math.min(currentPage * PAGE_SIZE, total);
  info.textContent = total ? `${{start}}–${{end}} van ${{total}}` : 'Geen resultaten';

  let html = `<button class="page-btn" onclick="goPage(${{currentPage-1}})" ${{currentPage===1?'disabled':''}}>‹</button>`;
  for (let p = Math.max(1, currentPage-2); p <= Math.min(pages, currentPage+2); p++) {{
    html += `<button class="page-btn${{p===currentPage?' active':''}}" onclick="goPage(${{p}})">${{p}}</button>`;
  }}
  html += `<button class="page-btn" onclick="goPage(${{currentPage+1}})" ${{currentPage>=pages?'disabled':''}}>›</button>`;
  btns.innerHTML = html;
}}

function goPage(p) {{
  const pages = Math.ceil(filteredData.length / PAGE_SIZE);
  if (p < 1 || p > pages) return;
  currentPage = p;
  renderTable();
}}

// ─── Sorteren ──────────────────────────────────────────────────────────────────
function sortTable(col) {{
  if (sortCol === col) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
  else {{ sortCol = col; sortDir = 'asc'; }}
  document.querySelectorAll('th').forEach(th => th.classList.remove('sort-asc','sort-desc'));
  const ths = document.querySelectorAll('th');
  const colMap = ['naam','status','locatie','beheerder','os','tools_status','update_schema','laatste_reboot','host_boot','backup_flag','backup_datum','ip_adres','vcpu','sql_versie','ha_beschermd','functie'];
  const idx = colMap.indexOf(col);
  if (idx >= 0) ths[idx].classList.add(sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
  filteredData.sort((a, b) => {{
    let av = a[col] ?? '', bv = b[col] ?? '';
    if (typeof av === 'number' && typeof bv === 'number') return sortDir === 'asc' ? av - bv : bv - av;
    return sortDir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
  }});
  currentPage = 1;
  renderTable();
}}

// ─── Filters ───────────────────────────────────────────────────────────────────
function applyFilters() {{
  const search   = document.getElementById('filter-search').value.toLowerCase();
  const locatie  = document.getElementById('filter-locatie').value;
  const status   = document.getElementById('filter-status').value;
  const beheerder = document.getElementById('filter-beheerder').value;
  const backup   = document.getElementById('filter-backup').value;
  const soort    = document.getElementById('filter-soort').value;

  filteredData = VM_DATA.filter(vm => {{
    if (locatie   && vm.locatie    !== locatie)   return false;
    if (status    && vm.status     !== status)    return false;
    if (beheerder && vm.beheerder  !== beheerder) return false;
    if (backup    && vm.backup_flag !== backup)   return false;
    if (soort     && vm.soort      !== soort)     return false;
    if (search) {{
      const haystack = [vm.naam, vm.ip_adres, vm.os, vm.beheerder, vm.functie, vm.locatie].join(' ').toLowerCase();
      if (!haystack.includes(search)) return false;
    }}
    return true;
  }});
  currentPage = 1;
  renderTable();
}}

function resetFilters() {{
  ['filter-search','filter-locatie','filter-status','filter-beheerder','filter-backup','filter-soort'].forEach(id => {{
    const el = document.getElementById(id);
    if (el.tagName === 'INPUT') el.value = '';
    else el.selectedIndex = 0;
  }});
  filteredData = [...VM_DATA];
  currentPage  = 1;
  renderTable();
}}

// ─── Alerts ────────────────────────────────────────────────────────────────────
function renderAlerts() {{
  const body  = document.getElementById('alerts-body');
  const count = document.getElementById('alert-count');
  const total = ALERTS.reduce((s, a) => s + a.items.length, 0);
  count.textContent = total + ' item' + (total !== 1 ? 's' : '');

  body.innerHTML = ALERTS.map(a => `
    <div class="alert-item ${{a.type}}">
      <div class="alert-title">${{a.icon}} ${{a.title}}</div>
      <ul>${{a.items.slice(0, 8).map(i => `<li>${{i}}</li>`).join('')}}${{a.items.length > 8 ? `<li>... en ${{a.items.length - 8}} meer</li>` : ''}}</ul>
    </div>`).join('');
}}

let alertsOpen = false;
function toggleAlerts() {{
  alertsOpen = !alertsOpen;
  document.getElementById('alerts-body').classList.toggle('alert-collapsed', !alertsOpen);
  document.getElementById('alert-toggle').textContent = alertsOpen ? '▲ Verberg' : '▼ Toon';
}}

// ─── Charts (via CDN) ──────────────────────────────────────────────────────────
function loadCharts() {{
  const script = document.createElement('script');
  script.src = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js';
  script.onload = drawCharts;
  script.onerror = () => {{ document.querySelectorAll('.chart-wrap').forEach(el => el.innerHTML = '<span style="color:#a0aec0;font-size:11px">Grafieken niet beschikbaar (geen internet)</span>'); }};
  document.head.appendChild(script);
}}

function drawCharts() {{
  const COLORS = {{
    green:  ['#48bb78','#276749'],
    red:    ['#fc8181','#9b2c2c'],
    blue:   ['#63b3ed','#2b6cb0','#bee3f8','#2c5282'],
    mixed:  ['#48bb78','#fc8181','#fbd38d','#90cdf4','#e2e8f0'],
  }};

  new Chart(document.getElementById('chart-status'), {{
    type: 'doughnut',
    data: {{ labels: CHART_DATA.status.labels, datasets: [{{ data: CHART_DATA.status.data, backgroundColor: COLORS.mixed, borderWidth: 2 }}] }},
    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'right', labels: {{ font: {{ size: 11 }} }} }} }} }}
  }});

  new Chart(document.getElementById('chart-backup'), {{
    type: 'doughnut',
    data: {{ labels: CHART_DATA.backup.labels, datasets: [{{ data: CHART_DATA.backup.data, backgroundColor: ['#48bb78','#fc8181'], borderWidth: 2 }}] }},
    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'right', labels: {{ font: {{ size: 11 }} }} }} }} }}
  }});

  new Chart(document.getElementById('chart-tools'), {{
    type: 'doughnut',
    data: {{ labels: CHART_DATA.tools.labels, datasets: [{{ data: CHART_DATA.tools.data, backgroundColor: COLORS.mixed, borderWidth: 2 }}] }},
    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'right', labels: {{ font: {{ size: 11 }} }} }} }} }}
  }});
}}

// ─── Init ──────────────────────────────────────────────────────────────────────
renderKPIs();
renderAlerts();
renderTable();
loadCharts();
</script>
</body>
</html>"""
    return html


# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    df1 = load_file1()
    df2 = load_file2()
    df  = pd.concat([df1, df2], ignore_index=True)

    print(f"Totaal VMs geladen: {len(df)} ({len(df1)} PMC + {len(df2)} RAM DC)")

    kpis       = compute_kpis(df)
    chart_data = compute_chart_data(df)
    alerts     = compute_alerts(df)

    print(f"KPIs: {kpis}")
    print(f"Aandachtspunten: {len(alerts)} categorieën")

    html = render_html(df, kpis, chart_data, alerts)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Dashboard geschreven naar: {OUTPUT}")
    print(f"  Open in browser: open '{OUTPUT}'")


if __name__ == "__main__":
    main()
