from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional

import requests


# ---------------------------
# Helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _norm(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _pick_value(v: Any) -> str:
    """SF can return dict-like values sometimes; pull a useful string."""
    if isinstance(v, dict):
        for k in ("externalCode", "code", "value", "label", "name", "id"):
            if k in v and v[k] is not None:
                return str(v[k]).strip()
        return str(v).strip()
    return _norm(v)


def _is_active_from_empl_status(val: Any) -> bool:
    """
    EmpJob employment status / report style:
      Active, A => active
      everything else (terminated/retired/leave/etc) => inactive
    """
    s = _pick_value(val).strip().lower()
    if not s:
        return False
    if s in ("a", "active", "t", "true", "1"):
        return True
    if s.startswith("active") and not s.startswith("inactive"):
        return True
    if "active" in s and "inactive" not in s:
        return True
    return False


def _safe_get_all(sf, path: str, params: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Return list if SF accepts the query, else None.
    We only swallow HTTP 400 here (invalid $select/$expand); other errors should surface.
    """
    try:
        return sf.get_all(path, params)
    except requests.exceptions.HTTPError as e:
        # If it's 400, it's usually "bad field in $select" or "bad expand"
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 400:
            return None
        raise


def _build_map(rows: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    out = {}
    for r in rows or []:
        k = _norm(r.get(key))
        if k:
            out[k] = r
    return out


# ---------------------------
# Main
# ---------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ------------------------------------------------------------
    # 1) EmpJob core (SAFE fields only)  ✅ this should not 400
    # ------------------------------------------------------------
    base_empjob_select = (
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange"
    )

    jobs = sf.get_all(
        "/odata/v2/EmpJob",
        {"$select": base_empjob_select, "$filter": "effectiveLatestChange eq true"},
    )
    empjob_rows = len(jobs)

    # ------------------------------------------------------------
    # 2) Employee status (try variants WITHOUT breaking EmpJob core)
    # ------------------------------------------------------------
    employee_status_source = "fallback(User.status)"
    emp_status_by_user: Dict[str, Any] = {}

    # Try plain fields first (some tenants expose one of these)
    status_candidates = [
        # most common EC field name
        ({"$select": "userId,emplStatus,effectiveLatestChange", "$filter": "effectiveLatestChange eq true"}, "EmpJob.emplStatus"),
        # some tenants / reports may use other naming
        ({"$select": "userId,employmentStatus,effectiveLatestChange", "$filter": "effectiveLatestChange eq true"}, "EmpJob.employmentStatus"),
        ({"$select": "userId,employeeStatus,effectiveLatestChange", "$filter": "effectiveLatestChange eq true"}, "EmpJob.employeeStatus"),
    ]

    status_rows = None
    status_label = None
    for params, label in status_candidates:
        status_rows = _safe_get_all(sf, "/odata/v2/EmpJob", params)
        if status_rows is not None:
            status_label = label
            break

    # Try navigation if plain field not available (requires $expand)
    if status_rows is None:
        nav_candidates = [
            ({"$select": "userId,emplStatusNav/externalCode,effectiveLatestChange",
              "$expand": "emplStatusNav",
              "$filter": "effectiveLatestChange eq true"}, "EmpJob.emplStatusNav/externalCode"),
            ({"$select": "userId,emplStatusNav/name,effectiveLatestChange",
              "$expand": "emplStatusNav",
              "$filter": "effectiveLatestChange eq true"}, "EmpJob.emplStatusNav/name"),
        ]
        for params, label in nav_candidates:
            status_rows = _safe_get_all(sf, "/odata/v2/EmpJob", params)
            if status_rows is not None:
                status_label = label
                break

    if status_rows is not None and status_label is not None:
        employee_status_source = status_label
        for r in status_rows:
            uid = _norm(r.get("userId"))
            if not uid:
                continue
            # figure out which field is present
            if "emplStatus" in r:
                emp_status_by_user[uid] = r.get("emplStatus")
            elif "employmentStatus" in r:
                emp_status_by_user[uid] = r.get("employmentStatus")
            elif "employeeStatus" in r:
                emp_status_by_user[uid] = r.get("employeeStatus")
            else:
                # nav path can appear as dicts depending on client
                emp_status_by_user[uid] = r.get("emplStatusNav") or r.get("emplStatusNav/externalCode") or r.get("emplStatusNav/name")

    # ------------------------------------------------------------
    # 3) Users (for email hygiene + fallback active/inactive)
    # ------------------------------------------------------------
    users = sf.get_all("/odata/v2/User", {"$select": "userId,status,email,username"})
    user_by_id = _build_map(users, "userId")

    def is_active_user_fallback(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        return s in ("active", "t", "true", "1")

    # Active set:
    # - Prefer EmpJob status if available
    # - Else fallback to User.status
    active_ids = set()
    inactive_ids = set()

    if emp_status_by_user:
        for uid, st in emp_status_by_user.items():
            if _is_active_from_empl_status(st):
                active_ids.add(uid)
            else:
                inactive_ids.add(uid)
    else:
        # fallback
        for u in users:
            uid = _norm(u.get("userId"))
            if not uid:
                continue
            if is_active_user_fallback(u):
                active_ids.add(uid)
            else:
                inactive_ids.add(uid)

    active_users_count = len(active_ids)
    inactive_users_count = len(inactive_ids)

    inactive_users_sample = []
    if emp_status_by_user:
        # sample with employment status
        for uid in list(inactive_ids)[:MAX_SAMPLE]:
            inactive_users_sample.append(
                {"userId": uid, "emplStatus": _pick_value(emp_status_by_user.get(uid))}
            )
    else:
        # fallback sample
        for uid in list(inactive_ids)[:MAX_SAMPLE]:
            u = user_by_id.get(uid, {})
            inactive_users_sample.append(
                {"userId": uid, "userStatus": u.get("status"), "username": u.get("username")}
            )

    # ------------------------------------------------------------
    # 4) Email hygiene (on ACTIVE users only)
    # ------------------------------------------------------------
    active_user_objs = [user_by_id[uid] for uid in active_ids if uid in user_by_id]

    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_user_objs:
        uid = _norm(u.get("userId"))
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

    missing_email_count = sum(1 for u in active_user_objs if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ------------------------------------------------------------
    # 5) Org + Manager checks (from SAFE jobs list)
    # ------------------------------------------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in jobs:
        uid = _norm(j.get("userId"))
        mgr = j.get("managerId")

        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

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

    # ------------------------------------------------------------
    # 6) Contingent workers (try EmpEmployment first, else EmpJob fallback)
    # ------------------------------------------------------------
    contingent_workers_count = 0
    contingent_workers_sample = []
    contingent_source = "not-available"

    # First try EmpEmployment.isContingentWorker (best)
    empemployment_rows = _safe_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
    )

    if empemployment_rows is not None:
        contingent_source = "EmpEmployment.isContingentWorker"
        contingent_ids = set()
        for r in empemployment_rows:
            uid = _norm(r.get("userId"))
            if uid:
                contingent_ids.add(uid)
                if len(contingent_workers_sample) < MAX_SAMPLE:
                    contingent_workers_sample.append({"userId": uid, "isContingentWorker": True})
        contingent_workers_count = len(contingent_ids)
    else:
        # Fallback: try EmpJob class/type fields in a SEPARATE call (won't break base jobs)
        empjob_cont_rows = None
        cont_select_candidates = [
            ("userId,employeeClass,effectiveLatestChange", "EmpJob.employeeClass"),
            ("userId,employeeType,effectiveLatestChange", "EmpJob.employeeType"),
            ("userId,employmentType,effectiveLatestChange", "EmpJob.employmentType"),
            ("userId,employeeClass,employeeType,employmentType,effectiveLatestChange", "EmpJob.employeeClass/employeeType/employmentType"),
        ]
        for sel, label in cont_select_candidates:
            empjob_cont_rows = _safe_get_all(
                sf,
                "/odata/v2/EmpJob",
                {"$select": sel, "$filter": "effectiveLatestChange eq true"},
            )
            if empjob_cont_rows is not None:
                contingent_source = label + " (fallback)"
                break

        if empjob_cont_rows is not None:
            # Decide contingent if any of the available fields look contingent-ish
            def is_contingent_row(r: dict) -> bool:
                for k in ("employeeClass", "employeeType", "employmentType"):
                    if k in r:
                        s = _pick_value(r.get(k)).lower()
                        if s in ("c", "cw", "contingent", "contingent worker", "contractor", "external"):
                            return True
                        if "conting" in s or "contract" in s or "vendor" in s or "external" in s:
                            return True
                return False

            cont_ids = set()
            for r in empjob_cont_rows:
                uid = _norm(r.get("userId"))
                if not uid:
                    continue
                if is_contingent_row(r):
                    cont_ids.add(uid)
                    if len(contingent_workers_sample) < MAX_SAMPLE:
                        contingent_workers_sample.append(
                            {
                                "userId": uid,
                                "employeeClass": _pick_value(r.get("employeeClass")) if "employeeClass" in r else "",
                                "employeeType": _pick_value(r.get("employeeType")) if "employeeType" in r else "",
                                "employmentType": _pick_value(r.get("employmentType")) if "employmentType" in r else "",
                            }
                        )
            contingent_workers_count = len(cont_ids)

    # ------------------------------------------------------------
    # Risk score (simple & explainable)
    # ------------------------------------------------------------
    def pct(x: int, denom: int) -> float:
        return 0.0 if denom == 0 else round((x / denom) * 100, 2)

    missing_manager_pct = pct(missing_manager_count, empjob_rows)
    invalid_org_pct = pct(invalid_org_count, empjob_rows)
    missing_email_pct = pct(missing_email_count, max(1, active_users_count))

    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, active_users_count)) * 100))
    risk_score = min(100, risk)

    # ------------------------------------------------------------
    # Output
    # ------------------------------------------------------------
    return {
        "snapshot_time_utc": now.isoformat(),

        "active_users": active_users_count,
        "inactive_users": inactive_users_count,

        "empjob_rows": empjob_rows,
        "current_empjob_rows": empjob_rows,

        "contingent_workers": contingent_workers_count,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }
