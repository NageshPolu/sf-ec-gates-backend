from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple, Iterable


# -----------------------------
# Utilities
# -----------------------------
def is_blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def chunks(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _safe_get_all(sf, path: str, params: Dict[str, Any]) -> Tuple[List[dict], Optional[str]]:
    try:
        rows = sf.get_all(path, params)
        return rows or [], None
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def _or_filter_int(field: str, values: List[int], suffix: str = "") -> str:
    # (field eq 1 OR field eq 2 OR ...)
    return "(" + " or ".join([f"{field} eq {v}{suffix}" for v in values]) + ")"


def _status_display(code: Optional[int], name: Optional[str]) -> str:
    if code is None and not name:
        return ""
    if name and code is not None:
        return f"{name} ({code})"
    if name:
        return str(name)
    return str(code)


# -----------------------------
# User.status helpers (ONLY for inferring which emplStatus is "Active")
# -----------------------------
def _user_is_active(u: dict) -> bool:
    s = str(u.get("status", "")).strip().lower()
    return s in ("active", "t", "true", "1", "a")


# -----------------------------
# Try to resolve emplStatus optionId -> label
#   1) PicklistLabel (best)
#   2) PicklistOption + expand picklistLabels (works in some tenants even if PicklistLabel blocked)
# -----------------------------
def _fetch_status_labels(sf, option_ids: List[int], locale: str) -> Tuple[Dict[int, str], str]:
    if not option_ids:
        return {}, "no-status-codes"

    option_ids = sorted(set([int(x) for x in option_ids if str(x).isdigit()]))[:600]

    # 1) PicklistLabel direct
    labels: Dict[int, str] = {}
    for use_L_suffix in (False, True):
        suffix = "L" if use_L_suffix else ""
        ok_all = True

        for part in chunks(option_ids, 40):
            filt = f"locale eq '{locale}' and {_or_filter_int('optionId', part, suffix=suffix)}"
            rows, err = _safe_get_all(
                sf,
                "/odata/v2/PicklistLabel",
                {"$select": "optionId,label,locale", "$filter": filt},
            )
            if err:
                ok_all = False
                break

            for r in rows:
                try:
                    oid = int(r.get("optionId"))
                except Exception:
                    continue
                lab = r.get("label")
                if lab is not None and str(lab).strip():
                    labels[oid] = str(lab).strip()

        if ok_all and labels:
            return labels, f"PicklistLabel(locale={locale}, suffix={'L' if use_L_suffix else 'none'})"

    # 2) PicklistOption expand picklistLabels
    # Some tenants block PicklistLabel but allow PicklistOption + nav expansion.
    labels2: Dict[int, str] = {}
    for use_L_suffix in (False, True):
        suffix = "L" if use_L_suffix else ""
        ok_all = True

        for part in chunks(option_ids, 25):
            # picklistLabels is a nav collection; we filter after fetch
            filt = _or_filter_int("optionId", part, suffix=suffix)
            rows, err = _safe_get_all(
                sf,
                "/odata/v2/PicklistOption",
                {
                    "$select": "optionId,externalCode,picklistLabels/locale,picklistLabels/label",
                    "$expand": "picklistLabels",
                    "$filter": filt,
                },
            )
            if err:
                ok_all = False
                break

            for r in rows:
                try:
                    oid = int(r.get("optionId"))
                except Exception:
                    continue
                pls = r.get("picklistLabels") or r.get("picklistLabelsNav") or None
                if isinstance(pls, dict) and "results" in pls:
                    pls = pls.get("results")

                label_found = None
                if isinstance(pls, list):
                    # pick locale match first, else first label
                    for x in pls:
                        if str(x.get("locale", "")).strip() == locale and str(x.get("label", "")).strip():
                            label_found = str(x.get("label")).strip()
                            break
                    if not label_found:
                        for x in pls:
                            if str(x.get("label", "")).strip():
                                label_found = str(x.get("label")).strip()
                                break

                if label_found:
                    labels2[oid] = label_found

        if ok_all and labels2:
            return labels2, f"PicklistOption+picklistLabels(locale={locale}, suffix={'L' if use_L_suffix else 'none'})"

    return {}, "labels blocked/unavailable"


# -----------------------------
# Main gates
# -----------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10
    LOCALE = "en_US"

    errors: Dict[str, str] = {}

    # ---------------------------
    # USERS (needed for email hygiene + inferring active emplStatus code)
    # ---------------------------
    users, err_user = _safe_get_all(sf, "/odata/v2/User", {"$select": "userId,status,email,username"})
    if err_user:
        errors["User"] = err_user

    user_by_id: Dict[str, dict] = {}
    for u in users:
        uid = u.get("userId")
        if not is_blank(uid):
            user_by_id[str(uid)] = u

    # ---------------------------
    # EMPJOB latest (org + manager + emplStatus)
    # Keep selects minimal to avoid 400 errors.
    # ---------------------------
    empjob_selects = [
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange",
        "userId,managerId,company,businessUnit,division,department,location,emplStatus",
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
        "userId,managerId,company,businessUnit,division,department,location",
    ]

    jobs: List[dict] = []
    empjob_source = "EmpJob"
    for sel in empjob_selects:
        rows, err = _safe_get_all(
            sf,
            "/odata/v2/EmpJob",
            {"$select": sel, "$filter": "effectiveLatestChange eq true"},
        )
        if not err:
            jobs = rows
            empjob_source = f"EmpJob(select={sel})"
            break
        errors[f"EmpJob({sel})"] = err

    # If filter causes issues, try no filter
    if not jobs:
        for sel in empjob_selects:
            rows, err = _safe_get_all(sf, "/odata/v2/EmpJob", {"$select": sel})
            if not err and rows:
                jobs = rows
                empjob_source = f"EmpJob(no-filter, select={sel})"
                break
            if err:
                errors[f"EmpJob(no-filter,{sel})"] = err

    empjob_rows = len(jobs)

    # Pull emplStatus codes
    empl_codes: List[int] = []
    for j in jobs:
        v = j.get("emplStatus")
        if v is None:
            continue
        try:
            empl_codes.append(int(v))
        except Exception:
            s = str(v).strip()
            if s.isdigit():
                empl_codes.append(int(s))

    # ---------------------------
    # Resolve status labels (best-effort)
    # ---------------------------
    status_labels, status_label_source = _fetch_status_labels(sf, empl_codes, LOCALE)

    # Attach derived fields to each job
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

    # ---------------------------
    # Infer which emplStatus code is "Active" when labels are blocked
    # Strategy:
    #   - look at users whose User.status is active
    #   - among their EmpJob rows, take most common emplStatus code
    # This avoids "inactive=0" problem completely.
    # ---------------------------
    empjob_by_uid: Dict[str, dict] = {}
    for j in jobs:
        uid = j.get("userId")
        if not is_blank(uid):
            empjob_by_uid[str(uid)] = j

    inferred_active_code: Optional[int] = None
    inferred_active_name: Optional[str] = None

    # If labels exist, prefer the label match for "Active"
    if status_labels:
        for code, name in status_labels.items():
            if str(name).strip().lower() == "active":
                inferred_active_code = code
                inferred_active_name = name
                break

    if inferred_active_code is None:
        candidates: List[int] = []
        for uid, u in user_by_id.items():
            if not _user_is_active(u):
                continue
            j = empjob_by_uid.get(uid)
            if not j:
                continue
            c = j.get("_emplStatusCode")
            if isinstance(c, int):
                candidates.append(c)

        if candidates:
            inferred_active_code = Counter(candidates).most_common(1)[0][0]
            inferred_active_name = status_labels.get(inferred_active_code)

    # If still none (no users or no codes), fall back to most common emplStatus in EmpJob
    if inferred_active_code is None and empl_codes:
        inferred_active_code = Counter(empl_codes).most_common(1)[0][0]
        inferred_active_name = status_labels.get(inferred_active_code)

    # ---------------------------
    # Active / Inactive employees based on inferred active emplStatus code
    # ---------------------------
    active_jobs: List[dict] = []
    inactive_jobs: List[dict] = []

    if inferred_active_code is not None:
        for j in jobs:
            if j.get("_emplStatusCode") == inferred_active_code:
                active_jobs.append(j)
            else:
                inactive_jobs.append(j)
        employee_status_source = f"EmpJob.emplStatus (active_code inferred={inferred_active_code})"
        if inferred_active_name:
            employee_status_source += f" aka '{inferred_active_name}'"
    else:
        # absolute fallback (should be rare)
        # classify active via User.status (better than zeros, but not ideal)
        for j in jobs:
            uid = str(j.get("userId"))
            u = user_by_id.get(uid, {})
            if _user_is_active(u):
                active_jobs.append(j)
            else:
                inactive_jobs.append(j)
        employee_status_source = "fallback(User.status) (no emplStatus inference possible)"

    total_active = len(active_jobs)
    total_inactive = len(inactive_jobs)

    # Status distribution for sanity
    status_distribution = defaultdict(int)
    for j in jobs:
        k = j.get("_emplStatusDisplay") or str(j.get("_emplStatusCode") or "")
        status_distribution[k] += 1
    status_distribution = dict(sorted(status_distribution.items(), key=lambda x: x[1], reverse=True))

    # ---------------------------
    # Email hygiene for ACTIVE employees only (your 43 should show here)
    # ---------------------------
    active_uids = [str(j.get("userId")) for j in active_jobs if not is_blank(j.get("userId"))]

    missing_email_count = 0
    missing_email_sample = []
    email_to_users = defaultdict(list)

    for uid in active_uids:
        u = user_by_id.get(uid, {})
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            missing_email_count += 1
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append({"userId": uid, "email": email, "username": u.get("username")})
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
    # Org & manager checks (ACTIVE only)
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
                        "employeeStatus": j.get("_emplStatusDisplay"),
                        "emplStatusCode": j.get("_emplStatusCode"),
                        "emplStatusName": j.get("_emplStatusName"),
                        "managerId": mgr,
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
    # Inactive sample (NOW it will populate even if labels blocked)
    # ---------------------------
    inactive_users_sample = []
    for j in inactive_jobs[:MAX_SAMPLE]:
        inactive_users_sample.append(
            {
                "userId": j.get("userId"),
                "employeeStatus": j.get("_emplStatusDisplay"),
                "emplStatusCode": j.get("_emplStatusCode"),
                "emplStatusName": j.get("_emplStatusName"),
            }
        )

    # ---------------------------
    # Contingent workers (EmpEmployment.isContingentWorker) best-effort
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []
    contingent_source = "not-available"

    emp_emp, err_emp = _safe_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker,effectiveLatestChange", "$filter": "effectiveLatestChange eq true"},
    )
    if err_emp:
        emp_emp2, err2 = _safe_get_all(sf, "/odata/v2/EmpEmployment", {"$select": "userId,isContingentWorker"})
        if err2:
            errors["EmpEmployment"] = err2
        else:
            emp_emp = emp_emp2
            contingent_source = "EmpEmployment.isContingentWorker (no-filter)"
    else:
        contingent_source = "EmpEmployment.isContingentWorker"

    contingent_uids = set()
    for r in emp_emp:
        uid = r.get("userId")
        if is_blank(uid):
            continue
        flag = r.get("isContingentWorker")
        is_true = str(flag).strip().lower() in ("true", "t", "1", "yes", "y")
        if is_true:
            contingent_uids.add(str(uid))

    contingent_worker_count = len(contingent_uids)

    for uid in list(contingent_uids)[:MAX_SAMPLE]:
        j = empjob_by_uid.get(uid, {})
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
    # Risk score (ACTIVE base)
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
    # Output
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        "active_users": total_active,
        "inactive_users": total_inactive,
        "empjob_rows": empjob_rows,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "contingent_workers": contingent_worker_count,
        "contingent_source": contingent_source,

        "risk_score": risk_score,

        # sources + inference details (helps debugging your footer)
        "employee_status_source": employee_status_source,
        "empjob_source": empjob_source,
        "emplstatus_label_source": status_label_source,
        "active_emplStatus_code_inferred": inferred_active_code,
        "active_emplStatus_name_inferred": inferred_active_name,

        # drilldowns
        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "invalid_org_sample": invalid_org_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_manager_sample": missing_manager_sample,
        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # sanity + diagnostics
        "status_distribution": status_distribution,
        "errors": errors,
    }

    return metrics
