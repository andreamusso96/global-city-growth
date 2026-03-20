{% set analysis_id = var('constants')['MAIN_ANALYSIS_ID'] %}
{% set min_subregion_city_count = 50 %}

WITH min_city_count AS (
    SELECT  analysis_id,
            micro_region,
            MIN(n_cities) AS min_n_cities
    FROM {{ source('figure_data_prep', 'world_m49_size_growth_slopes') }}
    WHERE analysis_id = {{ analysis_id }}
    GROUP BY analysis_id, micro_region
),
valid_micro_regions AS (
    SELECT analysis_id, micro_region, min_n_cities
    FROM min_city_count
    WHERE micro_region != 'Other'
    AND min_n_cities >= {{ min_subregion_city_count }}
),
slopes AS (
    SELECT analysis_id, micro_region, year, size_growth_slope, n_cities
    FROM {{ source('figure_data_prep', 'world_m49_size_growth_slopes') }}
    WHERE analysis_id = {{ analysis_id }}
),
slopes_urbanization AS (
    SELECT  s.analysis_id,
            s.micro_region,
            s.year,
            s.size_growth_slope,
            s.n_cities,
            v.min_n_cities,
            u.urban_population_share
    FROM slopes s
    INNER JOIN valid_micro_regions v
    USING (analysis_id, micro_region)
    INNER JOIN {{ ref('world_m49_urbanization') }} u
    USING (analysis_id, micro_region, year)
)
SELECT *
FROM slopes_urbanization
ORDER BY micro_region, year
