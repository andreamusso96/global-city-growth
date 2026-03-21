WITH clean_variable_names AS (
    SELECT  UPPER(TRIM("Code")) AS country,
            "Year"::INT AS year,
            "Population share in urban areas"::DOUBLE PRECISION / 100.0 AS urban_population_share
    FROM {{ source('hyde', 'world_hyde_urbanization_raw') }}
),
drop_non_country_entities AS (
    SELECT  country,
            year,
            urban_population_share
    FROM clean_variable_names
    WHERE country IS NOT NULL
    AND country != ''
),
modify_special_country_names AS (
    SELECT  CASE
                WHEN country = 'OWID_SRM' THEN 'SRB'
                WHEN country = 'ROU' THEN 'ROM'
                WHEN country = 'COD' THEN 'ZAR'
                ELSE country
            END AS country,
            year,
            urban_population_share
    FROM drop_non_country_entities
)
SELECT  country,
        year,
        urban_population_share
FROM modify_special_country_names
ORDER BY country, year
