{% set analysis_id = var('constants')['MAIN_ANALYSIS_ID'] %}
{% set min_subregion_city_count = 50 %}

{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
) }}

WITH country_borders AS (
    SELECT  COALESCE(r.micro_region, 'Other') AS micro_region,
            b.geom
    FROM {{ ref('world_country_borders_2019') }} b
    INNER JOIN {{ source('cshapes', 'world_crosswalk_cshapes_code_to_iso_code') }} cw
    ON b.gwcode = cw.cshapes_code
    LEFT JOIN {{ ref('world_m49_region') }} r
    ON cw.world_bank_code = r.country
),
micro_region_borders AS (
    SELECT  micro_region,
            ST_CollectionExtract(ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))), 3) AS geom
    FROM country_borders
    GROUP BY micro_region
),
slope_summary AS (
    SELECT  analysis_id,
            micro_region,
            AVG(size_growth_slope) AS size_growth_slope,
            MIN(n_cities) AS min_n_cities
    FROM {{ source('figure_data_prep', 'world_m49_size_growth_slopes') }}
    WHERE analysis_id = {{ analysis_id }}
    GROUP BY analysis_id, micro_region
),
micro_region_borders_with_slopes AS (
    SELECT  {{ analysis_id }} AS analysis_id,
            b.micro_region,
            CASE
                WHEN b.micro_region = 'Other' THEN NULL
                WHEN COALESCE(s.min_n_cities, 0) < {{ min_subregion_city_count }} THEN NULL
                ELSE s.size_growth_slope
            END AS size_growth_slope,
            s.min_n_cities,
            CASE
                WHEN b.micro_region = 'Other' THEN FALSE
                WHEN COALESCE(s.min_n_cities, 0) < {{ min_subregion_city_count }} THEN FALSE
                ELSE TRUE
            END AS has_data,
            b.geom
    FROM micro_region_borders b
    LEFT JOIN slope_summary s
    USING (micro_region)
)
SELECT *
FROM micro_region_borders_with_slopes
ORDER BY micro_region
