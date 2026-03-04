# CNU Weekly Activation ETL Pipeline

## Overview

An end-to-end automated data pipeline that replaced a fully manual weekly process for tracking and actioning Concurrent Named User (CNU) software license activations. Built in Snowflake SQL, Power BI, and Power Automate.

---

## The Problem (Before)

Each week, a Business Objects report was emailed manually to the team. From there:

- A team member had to **manually review every activation** and determine which business category it belonged to
- They then had to **manually populate Salesforce tasks** using a plain-text template — copied line by line from Excel for every account and account owner
- Each **account rep then had to**:
  - Create a Salesforce case manually
  - Select the correct template and contacts
  - Format and send the customer email

**Time cost per week:**
- ~20 minutes for the person processing the report
- ~15 minutes per account rep receiving tasks
- Multiply across a team of 8 — this was hours of repetitive manual work every single week

---

## The Solution (After)

A fully automated ETL pipeline that:

1. **Extracts** raw CNU activation data from the Snowflake data warehouse
2. **Transforms** it using complex business logic to categorize every activation automatically
3. **Loads** results into a Power BI semantic model
4. **Triggers** a Power Automate flow that automatically creates Salesforce cases, assigns the correct account owner, pre-populates the email template, and notifies the customer

**Zero manual categorization. Zero copy-pasting. Cases created automatically.**

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Snowflake |
| Transformation | Snowflake SQL (CTEs) |
| Reporting | Power BI (Semantic Model) |
| Automation | Power Automate |
| Case Management | Salesforce |

---

## Data Sources

All data lives in `DW_PROD` in Snowflake across two source systems:

**Entitlement Data Warehouse**
- `MART_ENTITLEMENT.REPORT_ACTIVATION` — raw machine activation records
- `MART_ENTITLEMENT.REPORT_ACTIVATION_EVENT` — event details (who activated, when, event type)
- `COMMON_DIMENSIONS.ENTITLEMENT_DIM` — license and product information
- `COMMON_DIMENSIONS.ACCOUNT_DIM` — account and company information
- `COMMON_DIMENSIONS.PRODUCT_DIM` — product names

**Salesforce (via Snowflake mirror)**
- `SOURCE_SALESFORCE.AGREEMENT_ATTRIBUTES` — master license contracts
- `SOURCE_SALESFORCE.AGREEMENT_TERM_YEAR` — license pricing model and policy type
- `SOURCE_SALESFORCE.TECHNICAL_ENGAGEMENT__C` — Salesforce engagement owner assignment

---

## Query Logic — How It Works

The Snowflake query is structured as 9 chained CTEs (Common Table Expressions):

### 1. Date Window
Automatically resolves the current reporting week (Sunday → Saturday) without any manual input. Mirrors the legacy Business Objects behavior.

### 2. Valid License Filter
Enforces a policy allow-list — only includes licenses under `Usage` or `Fixed Fee Regular` pricing. Automatically excludes Secure, Headcount, and True-Up contract types.

### 3. Current Week Events
Pulls all CNU (Concurrent + Counted Named User) activation events for the reporting window. Filters out internal MathWorks accounts. Tags each product as Desktop or Server by base product code.

### 4. Host Canonicalization
Normalizes machine host IDs by tokenizing, sorting, and rejoining — so the same physical machine is never split into multiple rows due to formatting differences (extra spaces, token order, casing).

### 5. Pre-Week History
Pulls all prior CNU events for the same account/host combinations before this week. Used to determine whether a machine has ever been active before.

### 6 & 7. Status Flags
Calculates supporting flags for each row:
- Was this host previously active?
- Has this host ever run Server products?
- Did a MATLAB Desktop activation occur this week (the "anchor" signal)?

### 8. Salesforce Owner Lookup
Identifies the most recent Salesforce Technical Engagement owner for each account — including closed engagements — so every activation has an assigned owner.

### 9. Category Logic (The Core Business Logic)
Applies a 4-tier mutually exclusive categorization:

| Priority | Category | Condition |
|---|---|---|
| 0 | **New Desktop Product Activated** | MATLAB Desktop anchor exists this week — bundles ALL Desktop + Server on that host |
| 1 | **New Desktop Product Activation on Non-Desktop Product Server** | Desktop on a server-history host, no MATLAB ever, first-time active |
| 2 | **Enterprise Server Product Activated on Unique Server** | First-time Server host, no MATLAB anchor |
| 3 | **No Action Required** | Everything else |

Final output is deduplicated to the latest event per License + Host + Product combination.

---

## Impact

- Eliminated ~20 minutes of manual report processing per week
- Eliminated ~15 minutes of manual task creation per account rep per week
- Elminated ~80+ hours of manual work per year
- Removed risk of human categorization errors
- Cases are created and emails sent automatically — no rep action required to initiate customer outreach
- Power BI model allows historical week-over-week analysis with consistent categorization logic

---

## Key Engineering Decisions

- **All categorization happens in SQL** — not in Power BI — so historical weeks re-run the same logic consistently without data drift
- **Host canonicalization** prevents the same machine from appearing as multiple records due to minor formatting differences
- **Exclusive date range semantics** `[start, end)` make scheduling and Power BI binding robust regardless of time-of-day

---

## Notes on Sensitive Data

All table names, schema references, and account identifiers in this repository have been anonymized. Sample data used for demonstration purposes does not reflect real customer information.
