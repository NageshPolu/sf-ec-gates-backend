from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _norm(v) -> str:
    return "" if v is None else str(v).strip().lower()


def _pick_first_label(nav: dict) -> str:
    """
    Handle PicklistOption shapes like:
      emplStatusNav: { picklistLabels: { results: [ {label, locale}, ... ] } }
    or sometimes:
      emplStatusNav: { label: "Active" }
    """
    if not isinstance(nav, dict):
        return ""

    # simplest form
    lbl = nav.get("label")
    if lbl and str(lbl).strip():
        return str(lbl).strip()

    pl = nav.get("picklistLabels")
    if isinstance(pl, dict):
        res = pl.get("results")
        if isinstance(res, list) and res:
            # prefer en* label if present
            for r in res:
                try:
                    loc = (r.get("locale") or "").lower()
                    lab = (r.get("label") or "").strip()
                    if lab and (loc.startswith("en") or loc == "en_us"):
                        return lab
                except Exception:
                    pass
            # else first available
            for r in res:
                lab = (r.get("label") or "").strip()
                if lab:
                    return lab
    return ""


def run_ec_gates(sf, instance_url: str = "", api_base_url: str = "") -> dict:
    """
    EC health snapshot with drilldowns.
    Uses:
      - EmpJob (latest) for employee status + org + manager checks
      - User for email hygiene
      - EmpEmployment for contingent worker flag (isContingentWorker)
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ------------------------------------------------------------
    # 1) EmpJob (latest) — employee status labels best-effort
    # ------------------------------------------------------------
    base_select = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "effectiveLatestChange,emplStatus"
    )

    jobs = []
    empl_status_source = "EmpJob.emplStatus (codes only)"

    # Try richer picklist nav expansions
    try:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {
                "$select": base_select + ",emplStatusNav/externalCode,emplStatusNav/picklistLabels/label,emplStatusNav/picklistLabels/locale",
                "$expand": "emplStatusNav/picklistLabels",
                "$filter": "effectiveLatestChange eq true",
            },
        )
        empl_status_source = "EmpJob.emplStatusNav.picklistLabels (fallback emplStatus)"
    except Exception:
        # fallback: try simple expand
        try:
            jobs = sf.get_all(
                "/odata/v2/EmpJob",
                {
                    "$select": base_select + ",emplStatusNav/label",
                    "$expand": "emplStatusNav",
                    "$filter": "effectiveLatestChange eq true",
                },
            )
            empl_status_source = "EmpJob.emplStatusNav.label (fallback emplStatus)"
        except Exception:
            jobs = sf.get_all(
                "/odata/v2/EmpJob",
                {"$select": base_select, "$filter": "effectiveLatestChange eq true"},
            )
            empl_status_source = "EmpJob.emplStatus (codes only)"

    # HARD FAIL if no data
    if not jobs:
        raise RuntimeError(
            "EmpJob returned 0 rows. Check: API base URL, credentials, and OData/RBP permission to read EmpJob."
        )

    def status_code(j: dict) -> str:
        v = j.get("emplStatus")
        return "" if v is None else str(v).strip()

    def status_name(j: dict) -> str:
        nav = j.get("emplStatusNav")
        lbl = _pick_first_label(nav)
        return lbl.strip() if lbl else ""

    def status_display(j: dict) -> str:
        code = status_code(j)
        name = status_name(j)
        if name and code:
            return f"{name} ({code})"
        return name or code or "(blank)"

    # Define active status tokens (label-side)
    ACTIVE_LABEL_TOKENS = {"active", "paid leave", "unpaid leave"}
    # Note: code-side varies by tenant; we do NOT guess codes here.
    # If label not available, we treat status as "unknown" and classify via User.status fallback later.

    active_jobs = []
    inactive_jobs = []
    inactive_by_status = defaultdict(int)

    for j in jobs:
        disp = status_display(j)
        s = _norm(disp)
        if s in ACTIVE_LABEL_TOKENS:
            active_jobs.append(j)
        else:
            inactive_jobs.append(j)
            inactive_by_status[disp] += 1

    active_user_ids = {j.get("userId") for j in active_jobs if j.get("userId")}
    inactive_user_ids = {j.get("userId") for j in inactive_jobs if j.get("userId")}

    # ------------------------------------------------------------
    # 2) Users — email hygiene + fallback for active/inactive if labels missing
    # ------------------------------------------------------------
    users = sf.get_all("/odata/v2/User", {"$select": "userId,email,username,status"})
    user_map = {u.get("userId"): u for u in users if u.get("userId")}

    # If we couldn't classify any active users (because labels are blocked),
    # fall back to User.status = 'active' best-effort.
    employee_status_fallback_used = False
    if len(active_user_ids) == 0 and len(inactive_user_ids) == len({j.get("userId") for j in jobs if j.get("userId")}):
        # everyone ended up "inactive" because we only had codes and no labels
        employee_status_fallback_used = True
        active_user_ids = {uid for uid, u in user_map.items() if _norm(u.get("status")) in ("active", "a", "true", "1")}
        inactive_user_ids = set(user_map.keys()) - active_user_ids

    active_users = [user_map[uid] for uid in active_user_ids if uid in user_map]
    total_active = len(active_users)

    missing_email_sample = []
    email_to_users = defaultdict(list)

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
            dup_rows.append(
                {"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]}
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ------------------------------------------------------------
    # 3) Org + Manager checks (ACTIVE jobs only)
    # ------------------------------------------------------------
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
    # 4) Inactive users sample (show BOTH code + name)
    # ------------------------------------------------------------
    inactive_users_sample = []
    for j in inactive_jobs[:MAX_SAMPLE]:
        uid = j.get("userId")
        u = user_map.get(uid, {})
        inactive_users_sample.append(
            {
                "userId": uid,
                "emplStatusCode": status_code(j) or "",
                "emplStatusName": status_name(j) or "",
                "employeeStatus": status_display(j),
                "username": u.get("username"),
                "email": u.get("email"),
            }
        )

    inactive_user_count = len(inactive_user_ids)

    # ------------------------------------------------------------
    # 5) Contingent workers
    # ------------------------------------------------------------
    contingent_workers_sample = []
    contingent_user_ids = set()
    contingent_source = "EmpEmployment.isContingentWorker"

    try:
        cont_rows = sf.get_all(
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        )
        for r in cont_rows:
            uid = r.get("userId")
            if uid:
                contingent_user_ids.add(uid)

        job_by_uid = {j.get("userId"): j for j in jobs if j.get("userId")}
        for uid in list(contingent_user_ids)[:MAX_SAMPLE]:
            u = user_map.get(uid, {})
            j = job_by_uid.get(uid, {})
            contingent_workers_sample.append(
                {
                    "userId": uid,
                    "isContingentWorker": True,
                    "emplStatusCode": status_code(j) or "",
                    "emplStatusName": status_name(j) or "",
                    "employeeStatus": status_display(j),
                    "username": u.get("username"),
                    "email": u.get("email"),
                }
            )
    except Exception:
        contingent_source = "not-available (no EmpEmployment/isContingentWorker)"

    contingent_worker_count = len(contingent_user_ids)
    contingent_active_count = len(contingent_user_ids.intersection(active_user_ids))

    # ------------------------------------------------------------
    # 6) Risk score (based on ACTIVE count)
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # OUTPUT
    # ------------------------------------------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),
        "instance_url": instance_url or "",
        "api_base_url": api_base_url or "",

        "active_users": total_active,
        "empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        "inactive_user_count": inactive_user_count,
        "inactive_users_by_status": dict(inactive_by_status),

        "contingent_worker_count": contingent_worker_count,
        "contingent_workers_active": contingent_active_count,
        "contingent_source": contingent_source,

        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        "employee_status_source": empl_status_source,
        "employee_status_fallback_used": employee_status_fallback_used,
    }

    return metrics
