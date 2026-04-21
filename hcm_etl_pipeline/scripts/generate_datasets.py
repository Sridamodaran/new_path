"""
generate_datasets.py
--------------------
Generates two production-style CSV datasets with realistic column counts:
  1. employee_master    — 25 columns, 5 mandatory columns enforced
  2. payroll_transactions — 22 columns

Also generates .manifest files (MD-style) for each dataset.

Usage:
    python scripts/generate_datasets.py --output_dir /data/landing --rows 1000
"""

import os
import csv
import hashlib
import json
import argparse
import random
import string
from datetime import datetime, timedelta, date


# ─── Config ──────────────────────────────────────────────────────────────────
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

DEPARTMENTS = ["Engineering", "Finance", "HR", "Operations", "Legal", "Sales",
               "Marketing", "IT", "Compliance", "Risk"]
DESIGNATIONS = ["Analyst", "Senior Analyst", "Manager", "Senior Manager",
                "Director", "VP", "AVP", "Associate"]
EMPLOYMENT_TYPES = ["Permanent", "Contract", "Intern"]
COST_CENTERS = [f"CC{str(i).zfill(4)}" for i in range(1001, 1020)]
LOCATIONS = ["Mumbai", "Bangalore", "Chennai", "Hyderabad", "Delhi", "Pune"]
GENDERS = ["M", "F", "Other"]
STATUS = ["Active", "Inactive"]
COUNTRIES = ["India"]
NATIONALITIES = ["Indian"]


# ─── Helpers ─────────────────────────────────────────────────────────────────
def random_date(start_year=2000, end_year=2023) -> str:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")


def random_phone() -> str:
    return f"+91-{random.randint(7000000000, 9999999999)}"


def random_ssn() -> str:
    # Intentionally dirty: some will have dashes (raw format), cleaned in raw layer
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def random_email(name: str) -> str:
    domain = random.choice(["corp.com", "hcm.org", "enterprise.in"])
    return f"{name.lower().replace(' ', '.')}@{domain}"


def random_id(prefix: str, digits: int = 6) -> str:
    return f"{prefix}{str(random.randint(10**(digits-1), 10**digits - 1))}"


def inject_nulls_and_dirt(value: str, null_prob: float = 0.03, dirt_prob: float = 0.02):
    """Randomly inject nulls or dirty values to simulate real-world data issues."""
    r = random.random()
    if r < null_prob:
        return ""                     # blank → NULL in spark
    if r < null_prob + dirt_prob:
        return random.choice(["NA", "N/A", "NULL", "none", "  "])
    return value


# ─── Dataset 1: employee_master (25 columns, 5 mandatory) ────────────────────
EMPLOYEE_COLUMNS = [
    # MANDATORY (5) — validated strictly in raw layer
    "employee_id",          # M1 — PK, must not be NULL
    "first_name",           # M2 — must not be NULL
    "last_name",            # M3 — must not be NULL
    "department",           # M4 — must not be NULL, must be in allowed list
    "employment_status",    # M5 — must not be NULL, must be Active/Inactive

    # Optional columns
    "middle_name",
    "date_of_birth",
    "date_of_joining",
    "date_of_exit",
    "designation",
    "employment_type",
    "cost_center",
    "location",
    "gender",
    "nationality",
    "email",
    "phone",
    "manager_id",
    "grade",
    "ssn",                  # Intentionally dirty: some have dashes (cleaned in raw layer)
    "bank_account_number",
    "pan_number",
    "aadhar_number",
    "country",
    "created_ts",
]


def generate_employee_row(idx: int) -> dict:
    emp_id = random_id("EMP", 6)
    first = random.choice(["Arjun", "Priya", "Rahul", "Sunita", "Vikram",
                            "Kavitha", "Ravi", "Meena", "Suresh", "Deepa",
                            "Karthik", "Lakshmi", "Arun", "Nisha", "Mohan"])
    last = random.choice(["Kumar", "Sharma", "Patel", "Singh", "Rao",
                           "Nair", "Menon", "Iyer", "Joshi", "Reddy"])
    name = f"{first} {last}"

    row = {
        # Mandatory — low null injection for these
        "employee_id":      emp_id,
        "first_name":       inject_nulls_and_dirt(first, null_prob=0.01),
        "last_name":        inject_nulls_and_dirt(last, null_prob=0.01),
        "department":       inject_nulls_and_dirt(random.choice(DEPARTMENTS), null_prob=0.01),
        "employment_status": inject_nulls_and_dirt(random.choice(STATUS), null_prob=0.01),

        # Optional — higher null injection
        "middle_name":       inject_nulls_and_dirt(random.choice(["A", "B", "C", ""]), null_prob=0.30),
        "date_of_birth":     inject_nulls_and_dirt(random_date(1970, 1998)),
        "date_of_joining":   inject_nulls_and_dirt(random_date(2005, 2023)),
        "date_of_exit":      inject_nulls_and_dirt(random_date(2020, 2024) if random.random() < 0.1 else "", null_prob=0.0),
        "designation":       inject_nulls_and_dirt(random.choice(DESIGNATIONS)),
        "employment_type":   inject_nulls_and_dirt(random.choice(EMPLOYMENT_TYPES)),
        "cost_center":       inject_nulls_and_dirt(random.choice(COST_CENTERS)),
        "location":          inject_nulls_and_dirt(random.choice(LOCATIONS)),
        "gender":            inject_nulls_and_dirt(random.choice(GENDERS)),
        "nationality":       inject_nulls_and_dirt(random.choice(NATIONALITIES)),
        "email":             inject_nulls_and_dirt(random_email(name)),
        "phone":             inject_nulls_and_dirt(random_phone()),
        "manager_id":        inject_nulls_and_dirt(random_id("EMP", 6), null_prob=0.10),
        "grade":             inject_nulls_and_dirt(f"G{random.randint(1,8)}"),
        "ssn":               inject_nulls_and_dirt(random_ssn()),   # dirty: dashes present
        "bank_account_number": inject_nulls_and_dirt(str(random.randint(10**9, 10**12))),
        "pan_number":        inject_nulls_and_dirt(f"{''.join(random.choices(string.ascii_uppercase, k=5))}{random.randint(1000,9999)}{''.join(random.choices(string.ascii_uppercase, k=1))}"),
        "aadhar_number":     inject_nulls_and_dirt(str(random.randint(10**11, 10**12 - 1))),
        "country":           inject_nulls_and_dirt(random.choice(COUNTRIES)),
        "created_ts":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return row


# ─── Dataset 2: payroll_transactions (22 columns) ────────────────────────────
PAYROLL_COLUMNS = [
    "transaction_id",       # PK
    "employee_id",          # FK to employee_master
    "payroll_month",        # YYYY-MM
    "payroll_year",
    "basic_salary",
    "hra",                  # house rent allowance
    "special_allowance",
    "conveyance_allowance",
    "medical_allowance",
    "incentive_amount",
    "overtime_pay",
    "gross_salary",
    "pf_deduction",         # provident fund
    "esi_deduction",
    "income_tax_deduction",
    "loan_deduction",
    "other_deductions",
    "net_salary",
    "payment_mode",
    "payment_date",
    "payment_status",
    "created_ts",
]


def generate_payroll_row(emp_id: str, month_str: str) -> dict:
    basic = round(random.uniform(20000, 200000), 2)
    hra = round(basic * 0.40, 2)
    special = round(basic * 0.20, 2)
    conveyance = round(random.uniform(800, 3200), 2)
    medical = round(random.uniform(500, 2000), 2)
    incentive = round(random.uniform(0, 50000), 2) if random.random() > 0.5 else 0
    overtime = round(random.uniform(0, 10000), 2) if random.random() > 0.7 else 0
    gross = basic + hra + special + conveyance + medical + incentive + overtime

    pf = round(basic * 0.12, 2)
    esi = round(gross * 0.0075, 2) if gross <= 21000 else 0
    tax = round(gross * random.uniform(0.05, 0.30), 2)
    loan = round(random.uniform(0, 20000), 2) if random.random() > 0.7 else 0
    other_ded = round(random.uniform(0, 2000), 2)
    net = round(gross - pf - esi - tax - loan - other_ded, 2)

    yr, mo = month_str.split("-")
    pay_date = f"{yr}-{mo}-{random.randint(25,28)}"

    return {
        "transaction_id":       random_id("TXN", 8),
        "employee_id":          inject_nulls_and_dirt(emp_id, null_prob=0.005),
        "payroll_month":        month_str,
        "payroll_year":         yr,
        "basic_salary":         inject_nulls_and_dirt(str(basic)),
        "hra":                  inject_nulls_and_dirt(str(hra)),
        "special_allowance":    inject_nulls_and_dirt(str(special)),
        "conveyance_allowance": inject_nulls_and_dirt(str(conveyance)),
        "medical_allowance":    inject_nulls_and_dirt(str(medical)),
        "incentive_amount":     str(incentive),
        "overtime_pay":         str(overtime),
        "gross_salary":         inject_nulls_and_dirt(str(round(gross, 2))),
        "pf_deduction":         str(pf),
        "esi_deduction":        str(esi),
        "income_tax_deduction": inject_nulls_and_dirt(str(tax)),
        "loan_deduction":       str(loan),
        "other_deductions":     str(other_ded),
        "net_salary":           inject_nulls_and_dirt(str(net)),
        "payment_mode":         inject_nulls_and_dirt(random.choice(["NEFT", "IMPS", "Cheque"])),
        "payment_date":         inject_nulls_and_dirt(pay_date),
        "payment_status":       inject_nulls_and_dirt(random.choice(["Processed", "Pending", "Failed"])),
        "created_ts":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ─── Manifest generator ───────────────────────────────────────────────────────
def compute_md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def count_rows(filepath: str) -> int:
    with open(filepath, "r") as f:
        return sum(1 for _ in f) - 1   # exclude header


def write_manifest(filepath: str, dataset_name: str, version: str, run_date: str, output_dir: str):
    md5 = compute_md5(filepath)
    row_count = count_rows(filepath)
    file_size = os.path.getsize(filepath)
    filename = os.path.basename(filepath)

    manifest_content = f"""# Manifest — {dataset_name}

## File Metadata

| Field            | Value                        |
|------------------|------------------------------|
| dataset_name     | {dataset_name}               |
| version          | {version}                    |
| filename         | {filename}                   |
| run_date         | {run_date}                   |
| generated_at     | {datetime.now().isoformat()} |

## Validation Checksums

| Check            | Value                                    |
|------------------|------------------------------------------|
| checksum_algo    | MD5                                      |
| checksum         | {md5}                                    |
| expected_rows    | {row_count}                              |
| file_size_bytes  | {file_size}                              |

## Schema Contract Reference

| Field            | Value                                    |
|------------------|------------------------------------------|
| contract_version | {version}                                |
| contract_path    | conf/data_contracts/{dataset_name}_{version}.yaml |
| mandatory_cols   | See contract YAML                        |

## Pipeline Trigger

| Field            | Value                                    |
|------------------|------------------------------------------|
| airflow_dag      | {dataset_name}_pipeline_dag              |
| trigger_mode     | file_sensor                              |
| threshold_min    | 5                                        |

## Notes

- Upstream system must drop BOTH the CSV and this manifest file atomically.
- Pipeline will not start if manifest is missing.
- If checksum or row count does not match, pipeline moves file to quarantine.
"""

    manifest_filename = filename.replace(".csv", ".manifest")
    manifest_path = os.path.join(output_dir, manifest_filename)
    with open(manifest_path, "w") as f:
        f.write(manifest_content)

    # Also write a machine-readable JSON sidecar for Airflow/Spark to parse
    json_manifest = {
        "dataset_name": dataset_name,
        "version": version,
        "filename": filename,
        "run_date": run_date,
        "checksum_algo": "MD5",
        "checksum": md5,
        "expected_rows": row_count,
        "file_size_bytes": file_size,
        "generated_at": datetime.now().isoformat(),
    }
    json_path = manifest_path.replace(".manifest", "_manifest.json")
    with open(json_path, "w") as f:
        json.dump(json_manifest, f, indent=2)

    print(f"  Manifest  : {manifest_path}")
    print(f"  JSON meta : {json_path}")
    print(f"  MD5       : {md5}")
    print(f"  Rows      : {row_count}")
    print(f"  Size      : {file_size} bytes")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", default="\\wsl.localhost\Ubuntu\home\srid01\hcm\myproject\data\landing", help="Output directory")
    parser.add_argument("--rows", type=int, default=500, help="Number of employee rows")
    parser.add_argument("--run_date", default=datetime.now().strftime("%Y%m%d"), help="Run date YYYYMMDD")
    args = parser.parse_args()

    run_date = args.run_date
    emp_dir = os.path.join(args.output_dir, "employee_master")
    pay_dir = os.path.join(args.output_dir, "payroll_transactions")
    os.makedirs(emp_dir, exist_ok=True)
    os.makedirs(pay_dir, exist_ok=True)

    # ── Employee master ──
    emp_file = os.path.join(emp_dir, f"employee_master_{run_date}.csv")
    print(f"\n[1/2] Generating employee_master → {emp_file}")
    employee_ids = []
    with open(emp_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EMPLOYEE_COLUMNS)
        writer.writeheader()
        for i in range(args.rows):
            row = generate_employee_row(i)
            employee_ids.append(row["employee_id"])
            writer.writerow(row)
    print(f"  Written {args.rows} rows")
    write_manifest(emp_file, "employee_master", "v1", run_date, emp_dir)

    # ── Payroll transactions ──
    pay_file = os.path.join(pay_dir, f"payroll_transactions_{run_date}.csv")
    print(f"\n[2/2] Generating payroll_transactions → {pay_file}")
    months = ["2024-01", "2024-02", "2024-03"]
    with open(pay_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PAYROLL_COLUMNS)
        writer.writeheader()
        txn_count = 0
        for emp_id in employee_ids:
            month = random.choice(months)
            row = generate_payroll_row(emp_id, month)
            writer.writerow(row)
            txn_count += 1
    print(f"  Written {txn_count} rows")
    write_manifest(pay_file, "payroll_transactions", "v1", run_date, pay_dir)

    print("\nDone. Drop both CSV + manifest files together to the landing zone.")


if __name__ == "__main__":
    main()
