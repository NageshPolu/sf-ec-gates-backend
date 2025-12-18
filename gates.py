from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Iterable


# -----------------------------
# Small utilities
# -----------------------------
def is_blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v: Any) -> bool:
    """
    Treat common placeholder strings as "missing email" (tenant data hygiene).
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def chunks(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _safe_get_all(sf, path: str, params: Dict[str, Any]) -> Tuple[List[dict], Optional[str]]:
    """
    Best-effort fetch. Never raises; returns (rows, error_string_or_None).
    """
    try:
        rows = sf.get_all(path, params)
        return rows or [], None
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def _or_filter_int(field: str, values: List[int], suffix: str = "") -> str:
    """
    Builds: (field eq 1 OR field eq 2 OR ...)
    suffix can be 'L' if your tenant expects optionId literals like 3965L.
    """
    return "(" + " or ".join([f"{field} eq {v}{suffix}" for v in values]) + ")"


# -----------------------------
# Picklist label resolution (emplStatus is a picklist optionId in many tenants)
# -----------------------------
def _fetch_picklist_labels_for_option_ids(sf, option_ids: List[int], locale: str) -> Tuple[Dict[int, str], str]:
    """
    Try to resolve optionId -> label via PicklistLabel.
    If tenant blocks it, return empty map and a reason.
    """
    if not option_ids:
        return {}, "no-status-codes"

    # De-dupe and cap to avoid huge filters
    option_ids = sorted(set([int(x) for x in option_ids if str(x).isdigit()]))[:500]

    out: Dict[int, str] = {}

    # Some tenants accept optionId eq 3965, some require 3965L.
    # We'll try without L first, then with L, chunking to stay under URL limits.
    for use_L_suffix in (False, True):
        suffix = "L" if use_L_suffix else ""
        all_ok = True

        for part in chunks(option_ids, 40):
            filt = f"locale eq '{locale}' and {_or_filter_int('optionId', part, suffix=suffix)}"
            rows, err = _safe_get_all(
                sf,
                "/odata/v2/PicklistLabel",
                {"$select": "optionId,label,locale", "$filter": filt},
            )
            if err:
                all_ok = False
                break

            for r in rows:
                try:
                    oid = int(r.get("optionId"))
                except Exception:
                    continue
                label = r.get("label")
                if label is not None and str(label).strip() != "":
                    out[oid] = str(label).strip()

        if all_ok and out:
            return out, f"PicklistLabel(locale={locale}, suffix={'L' if use_L_suffix else 'none'})"

    return {}, "PicklistLabel blocked/unavailable"


def _status_display(code: Optional[int], name: Optional[str]) -> str:
    if code is None and not name:
        return ""
    if name and code is not None:
        return f"{name} ({code})"
    if name:
        return str(name)
    return str(code)


def _is_active_status(name: Optional[str], code: Optional[int]) -> bool:
    """
    Decide "Active" based on label first. If label isn't available, we cannot safely infer
    active from numeric codes across customers, so we treat unknown as active ONLY if the
    user.status says active later (fallback path).
    """
    if name and str(name).strip().lower() == "active":
        return True
    return False


# -----------------------------
# Main gate runner
# -----------------------------
def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).
    Sources (best effort):
      - EmpJob (effectiveLatestChange=true): org, manager, emplStatus (Employee Status)
      - PicklistLabel: emplStatus name resolution (optional)
      - User: email hygiene
      - EmpEmployment: contingent workers (isContingentWorker) (optional)
    """

    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10
    LOCALE = "en_US"

    errors: Dict[str, str] = {}

    # ---------------------------
    # 1) EmpJob latest (core truth for Active/Inactive + org + manager)
    # ---------------------------
    # We keep select minimal to avoid 400s due to tenant-specific fields.
    # IMPORTANT: emplStatus is what you want (Employee Status picklist optionId).
    empjob_selects = [
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange",
        "userId,managerId,company,businessUnit,division,department,location,emplStatus",
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
        "userId,managerId,company,businessUnit,division,department,location",
    ]

    jobs: List[dict] = []
    empjob_source = "EmpJob"
    for sel in empjob_selects:
        params = {"$select": sel, "$filter": "effectiveLatestChange eq true"}
        rows, err = _safe_get_all(sf, "/odata/v2/EmpJob", params)
        if not err:
            jobs = rows
            empjob_source = f"EmpJob(select={sel})"
            break
        errors[f"EmpJob({sel})"] = err

    # If still empty, retry without filter (some tenants dislike effectiveLatestChange filter)
    if not jobs:
        for sel in empjob_selects:
            params = {"$select": sel}
            rows, err = _safe_get_all(sf, "/odata/v2/EmpJob", params)
            if not err and rows:
                jobs = rows
                empjob_source = f"EmpJob(no-filter, select={sel})"
                break
            if err:
                errors[f"EmpJob(no-filter,{sel})"] = err

    empjob_rows = len(jobs)

    # Pull status codes from EmpJob
    empl_status_codes: List[int] = []
    for j in jobs:
        v = j.get("emplStatus")
        if v is None:
            continue
        try:
            empl_status_codes.append(int(v))
        except Exception:
            # some tenants might return string codes; try to parse digits
            s = str(v).strip()
            if s.isdigit():
                empl_status_codes.append(int(s))

    # ---------------------------
    # 2) Resolve emplStatus code -> label via PicklistLabel (best-effort)
    # ---------------------------
    status_labels, status_label_source = _fetch_picklist_labels_for_option_ids(sf, empl_status_codes, LOCALE)

    # Build job rows with status label fields
    for j in jobs:
        code = None
        try:
            if j.get("emplStatus") is not None:
                code = int(j.get("emplStatus"))
        except Exception:
            pass

        name = status_labels.get(code) if code is not None else None
        j["_emplStatusCode"] = code
        j["_emplStatusName"] = name
        j["_emplStatusDisplay"] = _status_display(code, name)

    # If we successfully got labels, classify active/inactive accurately.
    # If labels are blocked, we will fall back to User.status for active/inactive classification,
    # but still show the code.
    labels_available = bool(status_labels)

    # ---------------------------
    # 3) Users for email hygiene + fallback active/inactive
    # ---------------------------
    users, err_user = _safe_get_all(sf, "/odata/v2/User", {"$select": "userId,status,email,username"})
    if err_user:
        errors["User"] = err_user

    user_by_id: Dict[str, dict] = {}
    for u in users:
        uid = u.get("userId")
        if not is_blank(uid):
            user_by_id[str(uid)] = u

    def _user_is_active(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        return s in ("active", "t", "true", "1", "a")

    # ---------------------------
    # 4) Decide Active / Inactive employees
    # ---------------------------
    active_jobs: List[dict] = []
    inactive_jobs: List[dict] = []

    if labels_available:
        for j in jobs:
            code = j.get("_emplStatusCode")
            name = j.get("_emplStatusName")
            if _is_active_status(name, code):
                active_jobs.append(j)
            else:
                # if status missing, treat as inactive? safer to flag it inactive
                inactive_jobs.append(j)
        employee_status_source = f"EmpJob.emplStatus + PicklistLabel({LOCALE})"
    else:
        # Labels blocked => best-effort fallback:
        # Active = users where User.status indicates active AND user exists in EmpJob list.
        empjob_uids = [str(j.get("userId")) for j in jobs if not is_blank(j.get("userId"))]
        empjob_uid_set = set(empjob_uids)

        active_uid_set = set()
        inactive_uid_set = set()

        for uid in empjob_uid_set:
            u = user_by_id.get(uid)
            if u and _user_is_active(u):
                active_uid_set.add(uid)
            else:
                inactive_uid_set.add(uid)

        for j in jobs:
            uid = str(j.get("userId"))
            if uid in active_uid_set:
                active_jobs.append(j)
            else:
                inactive_jobs.append(j)

        employee_status_source = "EmpJob.emplStatus (labels blocked) → fallback(User.status)"

    total_active = len(active_jobs)
    total_inactive = len(inactive_jobs)

    # Status distribution (helpful for sanity)
    status_distribution = defaultdict(int)
    for j in jobs:
        d = j.get("_emplStatusDisplay") or str(j.get("_emplStatusCode") or "")
        status_distribution[d] += 1
    status_distribution = dict(sorted(status_distribution.items(), key=lambda x: x[1], reverse=True))

    # ---------------------------
    # 5) Email hygiene (ACTIVE employees only)
    # ---------------------------
    active_user_ids = [str(j.get("userId")) for j in active_jobs if not is_blank(j.get("userId"))]
    active_user_ids_set = set(active_user_ids)

    missing_email_sample = []
    email_to_users = defaultdict(list)

    missing_email_count = 0
    for uid in active_user_ids:
        u = user_by_id.get(uid, {})
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            missing_email_count += 1
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
        else:
            email_to_users[email_norm].append(uid)

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # 6) Org + Manager checks (ACTIVE employees only)
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in active_jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append(
                    {
                        "userId": uid,
                        "managerId": mgr,
                        "employeeStatus": j.get("_emplStatusDisplay"),
                        "emplStatusCode": j.get("_emplStatusCode"),
                        "emplStatusName": j.get("_emplStatusName"),
                    }
                )

        missing_fields = [k for k in ORG_FIELDS if is_blank(j.get(k))]
        if missing_fields:
            invalid_org_count += 1
            for f in missing_fields:
                org_missing_field_counts[f] += 1

            if len(invalid_org_sample) < MAX_SAMPLE:
                invalid_org_sample.append(
                    {
                        "userId": uid,
                        "employeeStatus": j.get("_emplStatusDisplay"),
                        "emplStatusCode": j.get("_emplStatusCode"),
                        "emplStatusName": j.get("_emplStatusName"),
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
    # 7) Inactive users sample (from EmpJob classification)
    # ---------------------------
    inactive_users_sample = []
    for j in inactive_jobs[:MAX_SAMPLE]:
        uid = j.get("userId")
        inactive_users_sample.append(
            {
                "userId": uid,
                "employeeStatus": j.get("_emplStatusDisplay"),
                "emplStatusCode": j.get("_emplStatusCode"),
                "emplStatusName": j.get("_emplStatusName"),
            }
        )

    # ---------------------------
    # 8) Contingent workers (best source: EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []
    contingent_source = "not-available"

    emp_employment_rows = []
    rows, err_emp_emp = _safe_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker,effectiveLatestChange", "$filter": "effectiveLatestChange eq true"},
    )
    if err_emp_emp:
        # try without filter
        rows2, err2 = _safe_get_all(sf, "/odata/v2/EmpEmployment", {"$select": "userId,isContingentWorker"})
        if err2:
            errors["EmpEmployment"] = err2
        else:
            emp_employment_rows = rows2
            contingent_source = "EmpEmployment.isContingentWorker (no-filter)"
    else:
        emp_employment_rows = rows
        contingent_source = "EmpEmployment.isContingentWorker"

    contingent_uids = set()
    for r in emp_employment_rows:
        uid = r.get("userId")
        if is_blank(uid):
            continue
        flag = r.get("isContingentWorker")
        is_true = str(flag).strip().lower() in ("true", "t", "1", "yes", "y")
        if is_true:
            contingent_uids.add(str(uid))

    contingent_worker_count = len(contingent_uids)

    # Provide sample with status display if we can
    job_by_uid = {}
    for j in jobs:
        uid = j.get("userId")
        if not is_blank(uid):
            job_by_uid[str(uid)] = j

    for uid in list(contingent_uids)[:MAX_SAMPLE]:
        j = job_by_uid.get(uid, {})
        contingent_workers_sample.append(
            {
                "userId": uid,
                "isContingentWorker": True,
                "employeeStatus": j.get("_emplStatusDisplay"),
                "emplStatusCode": j.get("_emplStatusCode"),
                "emplStatusName": j.get("_emplStatusName"),
            }
        )

    # ---------------------------
    # 9) Risk score (simple, explainable, based on ACTIVE employees)
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
    # OUTPUT (Streamlit-friendly keys)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # KPIs
        "active_users": total_active,
        "inactive_users": total_inactive,
        "empjob_rows": empjob_rows,
        "current_empjob_rows": empjob_rows,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "contingent_workers": contingent_worker_count,
        "contingent_source": contingent_source,

        "risk_score": risk_score,

        # Sources (for your footer text)
        "employee_status_source": employee_status_source,
        "empjob_source": empjob_source,
        "emplstatus_label_source": status_label_source,

        # Drilldowns
        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "invalid_org_sample": invalid_org_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_manager_sample": missing_manager_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # Extra diagnostics (optional)
        "status_distribution": status_distribution,
        "errors": errors,  # keep it; your UI can hide it unless debug toggle is on
    }

    return metrics
