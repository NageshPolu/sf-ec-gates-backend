from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple


# ---------------------------
# Small helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    """
    Treat common placeholder strings as "missing email" (tenant data hygiene).
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _as_bool(v: Any) -> bool:
    if v is True:
        return True
    if v is False:
        return False
    s = "" if v is None else str(v).strip().lower()
    return s in ("true", "t", "1", "yes", "y")


def _normalize_status(v: Any) -> str:
    return "" if v is None else str(v).strip().lower()


def _is_active_employee_status(v: Any) -> bool:
    """
    Robust Active detection for EmpEmployment.emplStatus / report-like values.

    Supports:
      - "A" / "a"
      - "Active"
      - boolean-ish "true"/"t"/"1"
    Avoids false-positive on "Inactive".
    """
    s = _normalize_status(v)
    if not s:
        return False

    # exact/short codes
    if s in ("a", "active", "t", "true", "1"):
        return True

    # string variants (but NOT inactive)
    if s.startswith("active"):
        return True
    if "active" in s and not s.startswith("inactive"):
        return True

    return False


def _safe_get_all(sf, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Wrapper so a missing entity/field doesn't crash the whole run.
    sf.get_all is assumed to raise on HTTP errors.
    """
    return sf.get_all(path, params)


# ---------------------------
# Main gate runner
# ---------------------------
def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).

    Sources:
      - EmpEmployment (emplStatus + isContingentWorker)  ✅ best match to your report columns
      - EmpJob (manager + org assignments)
      - User (email hygiene + duplicates)
    """
    now = datetime.now(timezone.utc)

    # Keep payload safe
    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # 1) Employment status + Contingent (EmpEmployment)
    # ---------------------------
    employee_status_source = "EmpEmployment.emplStatus"
    contingent_source = "EmpEmployment.isContingentWorker"

    employments: List[Dict[str, Any]] = []
    have_contingent_flag = False

    # Try best-case: latest employments including contingent flag
    try:
        employments = _safe_get_all(
            sf,
            "/odata/v2/EmpEmployment",
            {
                "$select": "userId,emplStatus,isContingentWorker,effectiveLatestChange",
                "$filter": "effectiveLatestChange eq true",
            },
        )
        have_contingent_flag = True
    except Exception:
        # Fallback: maybe isContingentWorker not exposed
        try:
            employments = _safe_get_all(
                sf,
                "/odata/v2/EmpEmployment",
                {
                    "$select": "userId,emplStatus,effectiveLatestChange",
                    "$filter": "effectiveLatestChange eq true",
                },
            )
            have_contingent_flag = False
            contingent_source = "not-available (EmpEmployment.isContingentWorker not exposed)"
        except Exception:
            # Ultimate fallback: no EmpEmployment at all -> derive active from User.status (less reliable)
            employments = []
            employee_status_source = "fallback(User.status)"
            contingent_source = "not-available (no EmpEmployment)"

    active_ids = set()
    inactive_rows = []

    contingent_worker_ids = set()
    contingent_workers_sample = []

    if employments:
        for e in employments:
            uid = e.get("userId")
            if is_blank(uid):
                continue

            status_val = e.get("emplStatus")
            if _is_active_employee_status(status_val):
                active_ids.add(str(uid).strip())
            else:
                inactive_rows.append(
                    {
                        "userId": uid,
                        "emplStatus": status_val,
                    }
                )

            if have_contingent_flag and _as_bool(e.get("isContingentWorker")):
                contingent_worker_ids.add(str(uid).strip())
                if len(contingent_workers_sample) < MAX_SAMPLE:
                    contingent_workers_sample.append(
                        {
                            "userId": uid,
                            "isContingentWorker": e.get("isContingentWorker"),
                            "emplStatus": status_val,
                        }
                    )

    # If EmpEmployment missing, fall back to User.status for active IDs (so app still works)
    if not employments:
        users_tmp = _safe_get_all(sf, "/odata/v2/User", {"$select": "userId,status"})
        for u in users_tmp:
            uid = u.get("userId")
            if is_blank(uid):
                continue
            s = _normalize_status(u.get("status"))
            if s in ("active", "t", "true", "1"):
                active_ids.add(str(uid).strip())
            else:
                inactive_rows.append({"userId": uid, "status": u.get("status")})

    inactive_user_count = len(inactive_rows)
    inactive_users_sample = inactive_rows[:MAX_SAMPLE]

    # ---------------------------
    # 2) User email hygiene (only for ACTIVE people)
    # ---------------------------
    users = _safe_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,email,username,status"},
    )

    user_by_id = {}
    for u in users:
        uid = u.get("userId")
        if is_blank(uid):
            continue
        user_by_id[str(uid).strip()] = u

    active_user_objs = [user_by_id[uid] for uid in active_ids if uid in user_by_id]
    total_active = len(active_user_objs)

    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_user_objs:
        uid = str(u.get("userId")).strip()
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

    missing_email_count = sum(
        1 for u in active_user_objs if is_missing_email_value(u.get("email"))
    )

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append(
                {
                    "email": email,
                    "count": len(uids),
                    "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL],
                }
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # 3) EmpJob org + manager checks (ACTIVE jobs only)
    # ---------------------------
    jobs = _safe_get_all(
        sf,
        "/odata/v2/EmpJob",
        {
            "$select": (
                "userId,managerId,company,businessUnit,division,department,location,"
                "effectiveLatestChange"
            ),
            "$filter": "effectiveLatestChange eq true",
        },
    )

    # only evaluate jobs for active people (prevents "everything becomes 0")
    active_jobs = [j for j in jobs if str(j.get("userId", "")).strip() in active_ids]

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in active_jobs:
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

    # ---------------------------
    # 4) Contingent workers count
    #    - If we have isContingentWorker: count ACTIVE contingent only
    # ---------------------------
    if have_contingent_flag:
        contingent_worker_count = sum(1 for uid in contingent_worker_ids if uid in active_ids)
    else:
        contingent_worker_count = 0  # no reliable flag -> leave 0 instead of guessing

    # ---------------------------
    # 5) Risk score (simple, explainable)
    # ---------------------------
    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # Output (keys your Streamlit expects)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # Top KPIs
        "active_users": total_active,
        "empjob_rows": len(active_jobs),           # what you really want to see for ACTIVE population
        "current_empjob_rows": len(active_jobs),   # alias

        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,

        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # Sources (shown as captions in your UI)
        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
