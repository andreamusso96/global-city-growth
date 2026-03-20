{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
) }}

WITH cbsa_base_geom AS (
    SELECT  cbsa_id AS cluster_id,
            year,
            0 AS urban_threshold,
            ST_Multi(
                ST_CollectionExtract(
                    ST_MakeValid(
                        ST_SimplifyPreserveTopology(geom, 1000.0)
                    ),
                    3
                )
            ) AS geom
    FROM {{ ref('usa_cbsa_geom') }}
    WHERE year IN (2010, 2020)
)
SELECT *
FROM cbsa_base_geom
