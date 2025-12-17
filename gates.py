from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------
# Small helpers
# -----------------------------
def is_blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def is_truthy(v: Any) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "t", "1", "y", "yes", "a", "active", "enabled", "enable")


def is_missing_email_value(v: Any) -> bool:
    """Treat common placeholder strings as 'missing email'."""
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def safe_lower(v: Any) -> str:
    return "" if v is None else str(v).strip().lower()


def pick_best_label(nav: Any) -> Optional[str]:
    """
    Try hard to extract a human label from a SuccessFactors picklist nav object.
    Different tenants/metadata can return slightly different shapes.
    """
    if not nav:
        return None

    # Sometimes nav is {"results":[{...}]} (rare for 1:1, but handle)
    if isinstance(nav, dict) and "results" in nav and isinstance(nav["results"], list):
        nav = nav["results"][0] if nav["results"] else None
        if not nav:
            return None

    if not isinstance(nav, dict):
        return None

    # Common direct keys
    for k in ("label_defaultValue", "name", "value", "externalName", "label"):
        v = nav.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Sometimes label is nested like: {"label": {"defaultValue": "Terminated"}}
    label_obj = nav.get("label")
    if isinstance(label_obj, dict):
        for k in ("defaultValue", "en_US", "en", "value"):
            v = label_obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    # Sometimes in localized structures
    for k in ("localizedLabel", "localizedName"):
        obj = nav.get(k)
        if isinstance(obj, dict):
            for kk, vv in obj.items():
                if isinstance(vv, str) and vv.strip():
                    return vv.strip()

    return None


def status_display(label: Optional[str], code: Optional[str]) -> str:
    c = "" if code is None else str(code).strip()
    l = "" if label is None else str(label).strip()
    if l and c:
        return f"{l} ({c})"
    if l:
        return l
    if c:
        return c
    return ""


# -----------------------------
# Robust fetch wrappers
# -----------------------------
def try_get_all(sf, path: str, params: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]], str]:
    """
    Calls sf.get_all safely.
    Returns: (ok, rows, err_msg)
    """
    try:
        rows = sf.get_all(path, params)
        return True, rows or [], ""
    except Exception as e:
        return False, [], str(e)


def fetch_empjob_latest(sf, max_sample: int) -> Tuple[List[Dict[str, Any]], str, bool]:
    """
    Fetch EmpJob latest rows, with best-effort to also fetch emplStatus label via emplStatusNav.
    Falls back if expand/select fails (400).
    Returns: (jobs, source_string, has_label)
    """
    base_select = "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange,emplStatus"
    expand = "emplStatusNav"
    # best attempt: include expand + label fields
    attempt1 = {
        "$select": base_select + ",emplStatusNav/externalCode,emplStatusNav/label_defaultValue,emplStatusNav/label",
        "$expand": expand,
        "$filter": "effectiveLatestChange eq true",
    }
    ok, jobs, err = try_get_all(sf, "/odata/v2/EmpJob", attempt1)
    if ok:
        return jobs, "EmpJob.emplStatus + emplStatusNav(label)", True

    # fallback: expand without explicit nav selects (some tenants dislike nav selects)
    attempt2 = {
        "$select": base_select,
        "$expand": expand,
        "$filter": "effectiveLatestChange eq true",
    }
    ok, jobs, err2 = try_get_all(sf, "/odata/v2/EmpJob", attempt2)
    if ok:
        return jobs, "EmpJob.emplStatus + emplStatusNav(best-effort)", True

    # fallback: no expand, just code
    attempt3 = {
        "$select": base_select,
        "$filter": "effectiveLatestChange eq true",
    }
    ok, jobs, err3 = try_get_all(sf, "/odata/v2/EmpJob", attempt3)
    if ok:
        return jobs, "EmpJob.emplStatus (code only)", False

    # total failure
    raise RuntimeError(f"EmpJob fetch failed. Errors: {err} | {err2} | {err3}")


def fetch_contingent_empemployment(sf, max_sample: int) -> Tuple[int, List[Dict[str, Any]], str]:
    """
    Count contingent workers from EmpEmployment.isContingentWorker (matches your report column).
    """
    # Try latest change filter first
    attempt1 = {
        "$select": "userId,isContingentWorker,effectiveLatestChange",
        "$filter": "effectiveLatestChange eq true",
    }
    ok, rows, err = try_get_all(sf, "/odata/v2/EmpEmployment", attempt1)

    if not ok:
        # fallback without effectiveLatestChange
        attempt2 = {"$select": "userId,isContingentWorker"}
        ok, rows, err2 = try_get_all(sf, "/odata/v2/EmpEmployment", attempt2)
        if not ok:
            return 0, [], "not-available (no EmpEmployment access)"

    contingent = []
    for r in rows:
        if is_truthy(r.get("isContingentWorker")):
            contingent.append({"userId": r.get("userId"), "isContingentWorker": r.get("isContingentWorker")})

    # Deduplicate by userId
    seen = set()
    uniq = []
    for r in contingent:
        uid = r.get("userId")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        uniq.append(r)

    return len(uniq), uniq[:max_sample], "EmpEmployment.isContingentWorker"


def user_is_active(u: Dict[str, Any]) -> bool:
    """
    Robust active detection for /odata/v2/User.status.
    Common tenant values seen: Active / Inactive, A / I, true/false, 1/0.
    """
    v = u.get("status")
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()

    # Most common
    if s in ("active", "a", "enabled", "enable", "true", "t", "1", "yes", "y"):
        return True
    if s in ("inactive", "i", "disabled", "disable", "false", "f", "0", "no", "n"):
        return False

    # Some tenants store "Active" with weird casing/spaces
    if "active" == s:
        return True
    if "inactive" == s:
        return False

    # Unknown -> be conservative (treat as inactive)
    return False


def employee_status_is_active(label: Optional[str], code: Optional[str]) -> bool:
    """
    Active vs inactive for workforce based on EmpJob emplStatus.
    If we have a label: Active means label == 'Active' (case-insensitive).
    If no label: treat certain codes as active-ish.
    """
    if label and label.strip().lower() == "active":
        return True
    c = "" if code is None else str(code).strip().lower()
    return c in ("active", "a", "1", "t", "true")


# -----------------------------
# Main gates function
# -----------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # USERS (Active + email hygiene)
    # ---------------------------
    ok_u, users, err_u = try_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )
    if not ok_u:
        raise RuntimeError(f"User fetch failed: {err_u}")

    active_users = [u for u in users if user_is_active(u)]
    total_active_users = len(active_users)

    # Missing emails + duplicate emails (for ACTIVE users)
    missing_email_sample: List[Dict[str, Any]] = []
    email_to_users: Dict[str, List[str]] = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
            continue

        email_to_users[email_norm].append(uid)

    missing_email_count = sum(1 for u in active_users if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (Latest) + Org + Managers + Employee Status (Inactive)
    # ---------------------------
    jobs, emp_status_source, has_status_label = fetch_empjob_latest(sf, MAX_SAMPLE)

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample: List[Dict[str, Any]] = []
    invalid_org_sample: List[Dict[str, Any]] = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    # Workforce status (inactive users derived from EmpJob emplStatus)
    inactive_employee_count = 0
    inactive_users_sample: List[Dict[str, Any]] = []
    status_counter = Counter()

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        # Missing manager
        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

        # Invalid org fields
        missing_fields = [k for k in ORG_FIELDS if is_blank(j.get(k))]
        if missing_fields:
            invalid_org_count += 1
            for f in missing_fields:
                org_missing_field_counts[f] += 1
            if len(invalid_org_sample) < MAX_SAMPLE:
                invalid_org_sample.append(
                    {
                        "userId": uid,
                        "missingFields": ", ".join(missing_fields),
                        "company": j.get("company"),
                        "businessUnit": j.get("businessUnit"),
                        "division": j.get("division"),
                        "department": j.get("department"),
                        "location": j.get("location"),
                        "managerId": mgr,
                    }
                )

        # Employee status label + code (emplStatusNav best-effort)
        code = j.get("emplStatus")
        label = None
        if has_status_label:
            label = pick_best_label(j.get("emplStatusNav"))
        disp = status_display(label, code)

        if disp:
            status_counter[disp] += 1

        # Inactive = not Active
        if not employee_status_is_active(label, code):
            inactive_employee_count += 1
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append({"userId": uid, "emplStatus": disp or (code or "")})

    # ---------------------------
    # CONTINGENT workers (EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count, contingent_workers_sample, contingent_source = fetch_contingent_empemployment(sf, MAX_SAMPLE)

    # ---------------------------
    # Percentages + Risk
    # ---------------------------
    def pct(x: int) -> float:
        return 0.0 if total_active_users == 0 else round((x / total_active_users) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active_users)) * 100))
    risk_score = min(100, risk)

    # top status breakdown sample (helps you verify quickly)
    status_breakdown_top = [{"emplStatus": k, "count": v} for k, v in status_counter.most_common(25)]

    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # KPIs expected by Streamlit
        "active_users": total_active_users,
        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # Workforce KPIs
        "inactive_users": inactive_employee_count,
        "inactive_user_count": inactive_employee_count,
        "employee_status_source": emp_status_source,

        # Contingent KPIs
        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,
        "contingent_source": contingent_source,

        # Drilldowns (tabs)
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # Debug/verification-friendly breakdown (safe, no secrets)
        "employee_status_breakdown_top": status_breakdown_top,
    }

    return metrics
