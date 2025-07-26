WITH customer_sample AS (
  SELECT *
  FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER
  LIMIT 1000
)
SELECT
  -- Unique identifier
  C_CUSTOMER_ID,
  -- Text cleanup (uppercase, trimming)
  UPPER(TRIM(C_FIRST_NAME))           AS FIRST_NAME_CLEAN,
  UPPER(TRIM(C_LAST_NAME))            AS LAST_NAME_CLEAN,
  -- Preferred customer flag (binary encoding)
  CASE WHEN C_PREFERRED_CUST_FLAG = 'Y' THEN 1 ELSE 0 END AS IS_PREFERRED_CUST,
  -- Date of birth features
  C_BIRTH_YEAR,
  C_BIRTH_MONTH,
  C_BIRTH_DAY,
  -- Age calculation (as of 2024)
  (2024 - C_BIRTH_YEAR)               AS AGE,
  -- Birth month as season (categorical encoding)
  -- 1 -> "Winter", 2 -> "Spring", 3 -> "Summer", 4 -> "Autumn"
  CASE 
    WHEN C_BIRTH_MONTH IN (12,1,2)   THEN 1
    WHEN C_BIRTH_MONTH IN (3,4,5)    THEN 2
    WHEN C_BIRTH_MONTH IN (6,7,8)    THEN 3
    WHEN C_BIRTH_MONTH IN (9,10,11)  THEN 4
    ELSE 0
  END                                AS BIRTH_SEASON,
  -- Age group categorization
  -- 1 -> Young, 2 -> Adult, 3 -> Senior
  CASE 
    WHEN (2024 - C_BIRTH_YEAR) < 25  THEN 1
    WHEN (2024 - C_BIRTH_YEAR) < 50  THEN 2
    ELSE 3
  END                                AS AGE_GROUP,
  -- Country normalization
  UPPER(TRIM(C_BIRTH_COUNTRY))       AS BIRTH_COUNTRY_CLEAN,
  -- Email/Username Handling
  COALESCE(C_EMAIL_ADDRESS, 'NO_EMAIL')   AS EMAIL_SAFE,
  -- Customer login flag
  CASE WHEN C_LOGIN IS NOT NULL THEN 1 ELSE 0 END AS HAS_LOGIN,
  -- Last review date as year (if column is populated as 'YYYY-MM-DD' or similar)
  TRY_TO_DATE(C_LAST_REVIEW_DATE)          AS LAST_REVIEW_DATE,
  EXTRACT(YEAR FROM TRY_TO_DATE(C_LAST_REVIEW_DATE)) AS LAST_REVIEW_YEAR,
  -- Days since last review (if applicable)
  DATEDIFF(
    'day', TRY_TO_DATE(C_LAST_REVIEW_DATE), CURRENT_DATE()
  )                                        AS DAYS_SINCE_LAST_REVIEW
FROM customer_sample;