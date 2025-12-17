from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List


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


def _norm_l(v: Any) -> str:
    return _norm(v).lower()


def _pick_value(v: Any) -> str:
    """
    EmpJob.emplStatus or other fields can sometimes come back as dict-like.
    We'll try to pull a useful string.
    """
    if isinstance(v, dict):
        # common SuccessFactors patterns
        for k in ("externalCode", "code", "value", "label", "name", "id"):
            if k in v and v[k] is not None:
                return str(v[k]).strip()
        # fallback
        return str(v).strip()
    return _norm(v)


def _is_active_status(status_val: Any) -> bool:
    """
    For EmpJob.emplStatus / report-like values:
      Active, A => active
      everything else => inactive (terminated/retired/paid leave/unpaid leave/discarded...)
    """
    s = _pick_value(status_val).strip().lower()
    if not s:
        return False
    if s in ("a", "active", "t", "true", "1"):
        return True
    # allow variants like "Active Employee"
    if s.startswith("active"):
        return True
    # avoid "inactive"
    if "active" in s and not s.startswith("inactive"):
        return True
    return False


def _is_contingent_from_empjob(j: dict) -> bool:
    """
    Best-effort fallback without EmpEmployment.isContingentWorker.

    Tries these EmpJob fields if present:
      - employeeClass
      - employeeType
      - employmentType

    Matches common tenant values/codes.
    """
    candidates = [
        j.get("employeeClass"),
        j.get("employeeType"),
        j.get("employmentType"),
    ]
    for raw in candidates:
        s = _pick_value(raw).strip().lower()
        if not s:
            continue

        # common codes/labels
        if s in ("c", "cw", "contingent", "contingent worker", "contractor", "external"):
            return True

        # tolerant substring checks
        if "conting" in s or "contract" in s or "vendor" in s or "external" in s:
            return True

    return False


# ---------------------------
# Main
# ---------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # EmpJob (Latest records) -> org checks + status + contingent fallback
    # ---------------------------
    # IMPORTANT: keep this broad so EmpJob rows stay correct (like your earlier 1575)
    empjob_select = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "emplStatus,effectiveLatestChange,"
        "employeeClass,employeeType,employmentType"
    )

    jobs = sf.get_all(
        "/odata/v2/EmpJob",
        {"$select": empjob_select, "$filter": "effectiveLatestChange eq true"},
    )

    empjob_rows = len(jobs)

    # Build active/inactive based on EmpJob.emplStatus
    active_ids = set()
    inactive_ids = set()
    inactive_users_sample = []

    for j in jobs:
        uid = _norm(j.get("userId"))
        if not uid:
            continue

        if _is_active_status(j.get("emplStatus")):
            active_ids.add(uid)
        else:
            inactive_ids.add(uid)
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append(
                    {"userId": uid, "emplStatus": _pick_value(j.get("emplStatus"))}
                )

    active_users_count = len(active_ids)
    inactive_users_count = len(inactive_ids)

    # Contingent workers (fallback via EmpJob fields)
    contingent_ids = set()
    contingent_workers_sample = []

    for j in jobs:
        uid = _norm(j.get("userId"))
        if not uid:
            continue
        if _is_contingent_from_empjob(j):
            contingent_ids.add(uid)
            if len(contingent_workers_sample) < MAX_SAMPLE:
                contingent_workers_sample.append(
                    {
                        "userId": uid,
                        "employeeClass": _pick_value(j.get("employeeClass")),
                        "employeeType": _pick_value(j.get("employeeType")),
                        "employmentType": _pick_value(j.get("employmentType")),
                    }
                )

    contingent_workers_count = len(contingent_ids)
    contingent_source = "EmpJob.employeeClass/employeeType/employmentType (fallback)"
    employee_status_source = "EmpJob.emplStatus"

    # ---------------------------
    # Org + manager checks (run across ALL latest EmpJob rows)
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in jobs:
        uid = _norm(j.get("userId"))
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
    # User email hygiene (ACTIVE population from EmpJob status)
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,email,username,status"},
    )

    user_by_id = {}
    for u in users:
        uid = _norm(u.get("userId"))
        if uid:
            user_by_id[uid] = u

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
    # Risk score (simple)
    # ---------------------------
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

    # ---------------------------
    # Output keys Streamlit expects
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        "active_users": active_users_count,
        "inactive_users": inactive_users_count,

        "empjob_rows": empjob_rows,
        "current_empjob_rows": empjob_rows,

        "contingent_workers": contingent_workers_count,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": pct(missing_manager_count, empjob_rows),

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": pct(invalid_org_count, empjob_rows),

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        # samples
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
