/*===============================================================================
CNU WEEKLY ACTIVATIONS – END‑TO‑END LOGIC WITH EXPLANATIONS 
-------------------------------------------------------------------------------
Purpose:
  Produce an accurate weekly activation report for CNU (Concurrent + Counted Named User)
  that:
    - Canonicalizes host IDs so formatting differences do not split machines.
    - Builds pre-week CNU history vs. in-week activity per (account, host).
    - Uses a Desktop "anchor": if any Desktop activation occurs on a host
      during the reporting week, then ALL Desktop + Server products on that host that
      week are grouped under "New Desktop Product Activated." (desktop+server bundling)
    - Preserves additional categories:
        * "New Desktop Product Activation on Non-Desktop Product Server"
        * "Enterprise Server Product Activated on Unique Server"
        * "No Action Required"
    - Enforces the Valid License allow‑list:
        * Include: Usage OR Fixed Fee Regular
        * Exclude: any term with "Secure" subtype (covers Secure / Headcount / True‑Up)
    - Emits only "activation‑life" events (ACTIVATED / RE‑ACTIVATED / MACHINE TRANSFER)
      and filters out immediate reversals to avoid noise.

Design notes:
  • All categorization happens in SQL (Snowflake), so viewing prior weeks in Power BI
    re-runs the same logic consistently—no "self‑contamination."
  • Week boundaries follow legacy BO: last completed calendar week (Sunday → Saturday).
  • Event date predicates use [start, end) semantics (end is exclusive), which is robust
    to time‑of‑day and makes Power BI binding straightforward later.
===============================================================================*/

WITH
/*===============================================================================
1) DATE WINDOW (BO‑STYLE FALLBACK)
-------------------------------------------------------------------------------
What:
  - Defines the reporting window as the last fully completed calendar week.
Why:
  - Mirrors BusinessObjects behavior where WEEK_SEQ_NBR = -1
  - Provides deterministic, scheduler-friendly outputs without user input.
How:
  - week_start: last week's Sunday
  - week_end  : this week's Sunday (exclusive upper bound)
Adjustments:
  - If your org ever changes to Monday‑based weeks, adjust DATE_TRUNC usage.
===============================================================================*/
ResolvedWindow AS (
  SELECT
    DATEADD('day', -7, DATE_TRUNC('week', CURRENT_DATE)) AS week_start,
    DATE_TRUNC('week', CURRENT_DATE)                     AS week_end   -- exclusive
),

/*===============================================================================
2) VALID LICENSES (ALLOW‑LIST)
-------------------------------------------------------------------------------
What:
  - Builds the list of master licenses allowed in the report.
Policy:
  - Include: Pricing model = 'Usage' OR ('Fixed Fee' AND subtype = 'Regular')
  - Exclude: ANY term year row where subtype LIKE '%Secure%' (covers Secure, Headcount,
             True‑Up/True Up, and related secure governance variants).
Why:
  - Ensures we do not notify or monitor secured/true‑up contracts that are out of scope.
Notes:
  - Uses AGREEMENT_ATTRIBUTES + AGREEMENT_TERM_YEAR in prod_db.source_crm.
  - Normalizes master license IDs by stripping leading zeros and uppercasing.
===============================================================================*/
ValidLicenses AS (
  SELECT DISTINCT 
    REGEXP_REPLACE(UPPER(TRIM(ML.MASTER_LICENSE__C::STRING)), '^0+', '') AS MLID_NORM
  FROM prod_db.source_crm.AGREEMENT_ATTRIBUTES ML
  JOIN prod_db.source_crm.AGREEMENT_TERM_YEAR ATY
    ON ML.ID = ATY.AGREEMENT_ATTRIBUTES__C
  WHERE (ATY.PRICING_MODEL_TYPE__C = 'Usage'
         OR (ATY.PRICING_MODEL_TYPE__C = 'Fixed Fee' 
             AND ATY.PRICING_MODEL_SUB_TYPE__C = 'Regular'))
    AND NOT EXISTS (
      SELECT 1 
      FROM prod_db.source_crm.AGREEMENT_TERM_YEAR ATY2
      WHERE ATY2.AGREEMENT_ATTRIBUTES__C = ML.ID
        AND ATY2.PRICING_MODEL_SUB_TYPE__C LIKE '%Secure%'
    )
),

/*===============================================================================
3) CURRENT WEEK EVENTS (CNU‑ONLY INTAKE)
-------------------------------------------------------------------------------
What:
  - Pulls current window's CNU activation events for all products on all hosts.
Filters:
  - Activation Type Name = 'Concurrent'
  - License Option Name  = 'Counted Named User'
  - Event date between [week_start, week_end)
  - Exclude internal company accounts by name pattern
Outputs:
  - Identifiers (account, license, product, base code)
  - Clean host string (lowercase; single spaces)
  - Activation labels, actor, timestamps, and standardized event_name (uppercase)
  - PROD_TYPE classification (Desktop vs Server) by base code list
  - Normalized master license for policy checks
Rationale:
  - Intake is kept broad across products; classification happens later.
===============================================================================*/
CurrentWeekBase AS (
  SELECT
      ad.CDS_ACCOUNT_ID,
      ad.ACCOUNT_GROUP_NAME,
      TRY_TO_NUMBER(ed.LICENSE_ID) AS LICENSE_ID_INT,
      pd.PRODUCT_NAME,
      ed.CORE_PRODUCT_BASE_CODE,
      /* Normalize raw host string (lowercase, single spaces) */
      REGEXP_REPLACE(LOWER(TRIM(ra.MACHINE_HOSTID)), '\\s+', ' ') AS CLEAN_HOST_STR,
      ra.ACTIVATION_DESCRIPTION                                   AS ACTIVATION_LABEL,
      rae."Performed by Contact Full Name Latin"                   AS ACTIVATED_BY,
      DATE(rae."Transaction Event Date")                           AS ACTIVATION_DATE,
      rae."Transaction Event Date"                                 AS ACTIVATION_TS,
      UPPER(rae."Transaction Event Name")                          AS EVENT_NAME,
      /* Desktop vs Server tagging by product base code */
      CASE WHEN ed.CORE_PRODUCT_BASE_CODE IN ('BS','MW','CS','DW','PR','MPS','MPR','WAP','CPB','OLN','MOS','OS')
           THEN 'Server' ELSE 'Desktop' END                        AS PROD_TYPE,
      REGEXP_REPLACE(UPPER(TRIM(ed.MASTER_LICENSE_ID::STRING)), '^0+', '') AS ED_MLID_NORM
  FROM prod_db.mart_entitlement.REPORT_ACTIVATION ra
  JOIN prod_db.mart_entitlement.REPORT_ACTIVATION_EVENT rae 
       ON ra.ACTIVATION_ID = rae."Activation ID"
  JOIN prod_db.common_dimensions.ENTITLEMENT_DIM ed 
       ON ra.ENTITLEMENT_ID = ed.ENTITLEMENT_ID
  JOIN prod_db.common_dimensions.ACCOUNT_DIM ad 
       ON ed.CDS_ACCOUNT_ID = ad.CDS_ACCOUNT_ID
  LEFT JOIN prod_db.common_dimensions.PRODUCT_DIM pd 
       ON ed.CORE_PRODUCT_BASE_CODE = pd.PRODUCT_BASE_CODE
  WHERE rae."Activation Type Name" = 'Concurrent'
    AND rae."License Option Name"   = 'Counted Named User'
    AND DATE(rae."Transaction Event Date") >= (SELECT week_start FROM ResolvedWindow)
    AND DATE(rae."Transaction Event Date") <  (SELECT week_end   FROM ResolvedWindow)
    AND ad.ACCOUNT_GROUP_NAME NOT ILIKE '%[Internal Company]%'
),

/*===============================================================================
4) CANONICAL HOST STRINGS
-------------------------------------------------------------------------------
What:
  - Converts CLEAN_HOST_STR into a canonical, stable key (CANON_HOST_STR).
How:
  - Tokenize by spaces → lowercase → remove empties → sort tokens → rejoin with single
    spaces. This collapses small formatting changes into a single signature.
Why:
  - Prevents fragmented rows when the same host name appears with extra spaces,
    underscores mixed with spaces, or transposed token order.
Output:
  - Same columns as CurrentWeekBase plus CANON_HOST_STR.
===============================================================================*/
CurrentWeekCanon AS (
  SELECT
      c.CDS_ACCOUNT_ID,
      c.ACCOUNT_GROUP_NAME,
      c.LICENSE_ID_INT,
      c.PRODUCT_NAME,
      c.CORE_PRODUCT_BASE_CODE,
      c.CLEAN_HOST_STR,
      c.ACTIVATION_LABEL,
      c.ACTIVATED_BY,
      c.ACTIVATION_DATE,
      c.ACTIVATION_TS,
      c.EVENT_NAME,
      c.PROD_TYPE,
      c.ED_MLID_NORM,
      LISTAGG(DISTINCT LOWER(TRIM(tok.VALUE)), ' ')
        WITHIN GROUP (ORDER BY LOWER(TRIM(tok.VALUE))) AS CANON_HOST_STR
  FROM CurrentWeekBase c
  CROSS JOIN LATERAL SPLIT_TO_TABLE(c.CLEAN_HOST_STR, ' ') tok
  WHERE TRIM(tok.VALUE) <> ''
  GROUP BY
      c.CDS_ACCOUNT_ID, c.ACCOUNT_GROUP_NAME, c.LICENSE_ID_INT, c.PRODUCT_NAME,
      c.CORE_PRODUCT_BASE_CODE, c.CLEAN_HOST_STR, c.ACTIVATION_LABEL, c.ACTIVATED_BY,
      c.ACTIVATION_DATE, c.ACTIVATION_TS, c.EVENT_NAME, c.PROD_TYPE, c.ED_MLID_NORM
),

/*===============================================================================
5) TARGET HOSTS FOR THIS WEEK
-------------------------------------------------------------------------------
What:
  - Minimizes history scans by restricting to (account, host) pairs seen this week.
===============================================================================*/
Targets AS (
  SELECT DISTINCT CDS_ACCOUNT_ID, CANON_HOST_STR 
  FROM CurrentWeekCanon
),

/*===============================================================================
6) PRE‑WEEK HISTORY (CNU ONLY)
-------------------------------------------------------------------------------
What:
  - Pulls all prior CNU events for the same (account, host) before week_start.
Why:
  - Determines whether the host was ever active before (for first‑time checks).
  - Distinguishes Desktop vs Server history for "Non‑Desktop Product Server" category.
Notes:
  - Uses the *same* host canonicalization applied to prior events for accurate joins.
  - Restricts to CNU (Concurrent + Counted Named User) for consistency with intake.
Outputs:
  - EH_PRODUCT_NAME, EH_PROD_TYPE, EVENT_NAME, EVENT_TS for history analysis.
===============================================================================*/
EventHistory AS (
  SELECT
    ad.CDS_ACCOUNT_ID,
    LISTAGG(DISTINCT LOWER(TRIM(tok.VALUE)), ' ')
        WITHIN GROUP (ORDER BY LOWER(TRIM(tok.VALUE))) AS CANON_HOST_STR,
    pd.PRODUCT_NAME              AS EH_PRODUCT_NAME,
    CASE WHEN ed.CORE_PRODUCT_BASE_CODE IN ('BS','MW','CS','DW','PR','MPS','MPR','WAP','CPB','OLN','MOS','OS')
         THEN 'Server' ELSE 'Desktop' END AS EH_PROD_TYPE,
    UPPER(rae."Transaction Event Name") AS EVENT_NAME,
    rae."Transaction Event Date"        AS EVENT_TS
  FROM prod_db.mart_entitlement.REPORT_ACTIVATION ra
  JOIN prod_db.mart_entitlement.REPORT_ACTIVATION_EVENT rae ON ra.ACTIVATION_ID = rae."Activation ID"
  JOIN prod_db.common_dimensions.ENTITLEMENT_DIM ed         ON ra.ENTITLEMENT_ID = ed.ENTITLEMENT_ID
  JOIN prod_db.common_dimensions.ACCOUNT_DIM ad              ON ed.CDS_ACCOUNT_ID = ad.CDS_ACCOUNT_ID
  LEFT JOIN prod_db.common_dimensions.PRODUCT_DIM pd         ON ed.CORE_PRODUCT_BASE_CODE = pd.PRODUCT_BASE_CODE
  JOIN Targets t                                              ON t.CDS_ACCOUNT_ID = ad.CDS_ACCOUNT_ID
  CROSS JOIN LATERAL SPLIT_TO_TABLE(
        REGEXP_REPLACE(LOWER(TRIM(ra.MACHINE_HOSTID)), '\\s+', ' '), ' ') tok
  WHERE TRIM(tok.VALUE) <> ''
    AND rae."Transaction Event Date" < (SELECT week_start FROM ResolvedWindow)
    AND rae."Activation Type Name" = 'Concurrent'
    AND rae."License Option Name"  = 'Counted Named User'
  GROUP BY 1,3,4,5,6
),

/*===============================================================================
7) STATUS & SUPPORTING FLAGS
-------------------------------------------------------------------------------
7a) StatusFlags: Was this host ever active (CNU) prior to this week?
    - Uses activation events set + RIGHTS REFRESH for "ever active" signal.
7b) HostHasServerHistory: Has this host ever run Server (pre‑week) or is it
    running Server this week? (used by Non‑Desktop Product Server rule)
7c) HasDesktopAnchorThisWeek: Did any Desktop activation event occur
    this week? (anchor detection)
7d) DesktopAnchorsWeek: The set of (account, host) for which an anchor exists.
===============================================================================*/
StatusFlags AS (
  SELECT
    c.*,
    MAX(CASE WHEN h.EVENT_NAME IN ('ACTIVATED','RE-ACTIVATED','MACHINE TRANSFER','RIGHTS REFRESH')
             THEN 1 ELSE 0 END)
      OVER (PARTITION BY c.CDS_ACCOUNT_ID, c.CANON_HOST_STR) AS WAS_PREV_ACTIVE
  FROM CurrentWeekCanon c
  LEFT JOIN EventHistory h
    ON c.CDS_ACCOUNT_ID = h.CDS_ACCOUNT_ID 
   AND c.CANON_HOST_STR = h.CANON_HOST_STR
),
HostHasServerHistory AS (
  SELECT CDS_ACCOUNT_ID, CANON_HOST_STR, 1 AS HAS_SERVER_EVER
  FROM (
    SELECT DISTINCT CDS_ACCOUNT_ID, CANON_HOST_STR FROM EventHistory     WHERE EH_PROD_TYPE = 'Server'
    UNION
    SELECT DISTINCT CDS_ACCOUNT_ID, CANON_HOST_STR FROM CurrentWeekCanon WHERE PROD_TYPE    = 'Server'
  )
),
HasMatlabThisWeek AS (
  SELECT DISTINCT CDS_ACCOUNT_ID, CANON_HOST_STR, 1 AS HAS_MATLAB_THIS_WEEK
  FROM CurrentWeekCanon
  WHERE PROD_TYPE = 'Desktop' 
    AND PRODUCT_NAME ILIKE 'MATLAB%' 
    AND PRODUCT_NAME NOT ILIKE '%Server%'
    AND EVENT_NAME IN ('ACTIVATED','RE-ACTIVATED','MACHINE TRANSFER')
),
MatlabAnchorsWeek AS (
  SELECT CDS_ACCOUNT_ID, CANON_HOST_STR
  FROM HasMatlabThisWeek
),

/*===============================================================================
8) CRM ENGAGEMENT OWNER (LATEST, INCLUDING CLOSED)
-------------------------------------------------------------------------------
What:
  - Chooses the most recent owner for the Enterprise Account Group.
Why:
  - Ensures every activation is assigned even when Technical Engagement is Closed.
Method:
  - Rank by LASTMODIFIEDDATE descending and take rn = 1.
===============================================================================*/
EngagementOwner AS (
  SELECT 
      ENTERPRISE_ACCOUNT_GROUP_NAME__C AS ACCOUNT_GROUP_NAME,
      TRIM(REGEXP_REPLACE(OWNER_NAME__C, '<[^>]*>|&[a-z#0-9]+;', '')) AS CLEAN_NAME,
      ROW_NUMBER() OVER (
         PARTITION BY ENTERPRISE_ACCOUNT_GROUP_NAME__C 
         ORDER BY LASTMODIFIEDDATE DESC) AS rn
  FROM prod_db.source_crm.TECHNICAL_ENGAGEMENT__C
  WHERE ISDELETED = 0 
    AND STATUS__C IN ('Open','Standby','Closed')
)

/*===============================================================================
9) FINAL OUTPUT WITH CATEGORY LOGIC
-------------------------------------------------------------------------------
Priority of categories (mutually exclusive):
  0) Desktop anchor present for this (account, host) in the week
       → "New Desktop Product Activated"
       (Bundles ALL Desktop + Server on that host in the same week)
  1) Desktop on a Non‑Desktop Product Server
       → "New Desktop Product Activation on Non-Desktop Product Server"
       (Server history exists; NO Desktop anchor ever & none this week; host first‑time)
  2) First‑time Server host (no Desktop anchor this week)
       → "Enterprise Server Product Activated on Unique Server"
  3) Everything else
       → "No Action Required"

Additional controls:
  • Emit only activation‑life events (no immediate reversals).
  • Enforce ValidLicenses allow‑list in the outer WHERE.
  • QUALIFY to keep the latest (License, Host, Product) within the window.
===============================================================================*/
SELECT
  "Account Group Name",
  "Activation Category",
  "License Number",
  "Product",
  "Machine Host ID",
  "Activation Label",
  "Activated By",
  "Activation Date",
  "CRM Engagement Owner"
FROM (
  SELECT
    sf.ACCOUNT_GROUP_NAME AS "Account Group Name",

    /* =========================
       CATEGORY LOGIC (priority)
       ========================= */
    CASE
      /* 0) Desktop anchor this week → bundle EVERYTHING (Desktop + Server) */
      WHEN EXISTS (
             SELECT 1
             FROM MatlabAnchorsWeek a
             WHERE a.CDS_ACCOUNT_ID = sf.CDS_ACCOUNT_ID
               AND a.CANON_HOST_STR = sf.CANON_HOST_STR
        )
        THEN 'New Desktop Product Activated'

      /* 1) Desktop on server-history host, NO Desktop anchor ever & none this week,
            and host is first‑time active this week → Non‑Desktop Product Server */
      WHEN sf.PROD_TYPE = 'Desktop'
        AND COALESCE(s.HAS_SERVER_EVER,0)        = 1
        AND COALESCE(mtw.HAS_MATLAB_THIS_WEEK,0) = 0
        AND NOT EXISTS (
              SELECT 1 FROM EventHistory p
              WHERE p.CDS_ACCOUNT_ID = sf.CDS_ACCOUNT_ID 
                AND p.CANON_HOST_STR = sf.CANON_HOST_STR
                AND p.EH_PROD_TYPE   = 'Desktop'
                AND p.EH_PRODUCT_NAME ILIKE 'MATLAB%' 
                AND p.EH_PRODUCT_NAME NOT ILIKE '%Server%'
        )
        AND COALESCE(sf.WAS_PREV_ACTIVE,0) = 0
        THEN 'New Desktop Product Activation on Non-Desktop Product Server'

      /* 2) First‑time Server host (no Desktop anchor) → Unique Server */
      WHEN COALESCE(sf.WAS_PREV_ACTIVE,0) = 0 AND sf.PROD_TYPE = 'Server'
        THEN 'Enterprise Server Product Activated on Unique Server'

      /* 3) Everything else → No Action Required */
      ELSE 'No Action Required'
    END AS "Activation Category",

    sf.LICENSE_ID_INT AS "License Number",
    sf.PRODUCT_NAME    AS "Product",

    /* Present canonical host across multiple lines (one token per line) for readability */
    REPLACE(sf.CANON_HOST_STR, ' ', CHR(10)) AS "Machine Host ID",

    sf.ACTIVATION_LABEL AS "Activation Label",
    sf.ACTIVATED_BY     AS "Activated By",
    sf.ACTIVATION_DATE  AS "Activation Date",
    eo.CLEAN_NAME       AS "CRM Engagement Owner",

    /* Hidden technicals used for de‑dup and policy checks */
    sf.ED_MLID_NORM,
    sf.ACTIVATION_TS

  FROM StatusFlags sf
  LEFT JOIN EngagementOwner         eo  ON UPPER(TRIM(eo.ACCOUNT_GROUP_NAME)) = UPPER(TRIM(sf.ACCOUNT_GROUP_NAME)) AND eo.rn = 1
  LEFT JOIN HostHasServerHistory    s   ON s.CDS_ACCOUNT_ID = sf.CDS_ACCOUNT_ID AND s.CANON_HOST_STR = sf.CANON_HOST_STR
  LEFT JOIN HasMatlabThisWeek       mtw ON mtw.CDS_ACCOUNT_ID = sf.CDS_ACCOUNT_ID AND mtw.CANON_HOST_STR = sf.CANON_HOST_STR

  /* Emit activation‑life events only (exclude immediate reversals) */
  WHERE sf.EVENT_NAME IN ('ACTIVATED','MACHINE TRANSFER','RE-ACTIVATED')
) f

/* Enforce ValidLicenses allow‑list: only include rows whose ML appears in policy */
WHERE EXISTS (
  SELECT 1 FROM ValidLicenses v WHERE v.MLID_NORM = f.ED_MLID_NORM
)

/* De‑dupe: keep last event per (License, Host, Product) within the window */
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY "License Number","Machine Host ID","Product"
    ORDER BY ACTIVATION_TS DESC
) = 1

/* Reader-friendly sort */
ORDER BY "Account Group Name","Activation Category" DESC,"Machine Host ID";
