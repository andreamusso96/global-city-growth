WITH clean_variable_names AS (
    SELECT  UPPER(TRIM("ISO-alpha3 Code")) AS country,
            NULLIF(TRIM("Sub-region Name"), '') AS micro_region
    FROM {{ source('owid', 'world_m49_region_raw') }}
),
drop_nulls AS (
    SELECT country, micro_region
    FROM clean_variable_names
    WHERE country IS NOT NULL
    AND country != ''
    AND micro_region IS NOT NULL
),
modify_special_country_names AS (
    SELECT CASE
                WHEN country = 'OWID_SRM' THEN 'SRB'
                WHEN country = 'ROU' THEN 'ROM'
                WHEN country = 'COD' THEN 'ZAR'
                ELSE country
            END AS country,
            micro_region
    FROM drop_nulls
),
deduplicated AS (
    SELECT DISTINCT country, micro_region
    FROM modify_special_country_names
)
SELECT *
FROM deduplicated
ORDER BY country
