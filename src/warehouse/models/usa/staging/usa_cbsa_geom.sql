{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
) }}

WITH cbsa_2010 AS (
    SELECT  LPAD(TRIM(CAST("CBSAFP10" AS TEXT)), 5, '0') AS cbsa_id,
            "NAME10" AS cbsa_name,
            2010 AS year,
            ST_Multi(
                ST_CollectionExtract(
                    ST_MakeValid(ST_Transform(geometry, 5070)),
                    3
                )
            ) AS geom
    FROM {{ source('suburbanization', 'usa_cbsa_geom_2010_raw') }}
),
cbsa_2020 AS (
    SELECT  LPAD(TRIM(CAST("CBSAFP" AS TEXT)), 5, '0') AS cbsa_id,
            "NAME" AS cbsa_name,
            2020 AS year,
            ST_Multi(
                ST_CollectionExtract(
                    ST_MakeValid(ST_Transform(geometry, 5070)),
                    3
                )
            ) AS geom
    FROM {{ source('suburbanization', 'usa_cbsa_geom_2020_raw') }}
),
cbsa_all_years AS (
    SELECT * FROM cbsa_2010
    UNION ALL
    SELECT * FROM cbsa_2020
),
cbsa_geom AS (
    SELECT  cbsa_id,
            cbsa_name,
            year,
            ST_Multi(ST_Union(geom)) AS geom
    FROM cbsa_all_years
    WHERE cbsa_id IS NOT NULL
    AND geom IS NOT NULL
    AND NOT ST_IsEmpty(geom)
    GROUP BY cbsa_id, cbsa_name, year
)
SELECT *
FROM cbsa_geom
