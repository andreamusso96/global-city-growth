SELECT  country,
        year,
        analysis_id,
        size_growth_slope,
        urban_population_share
FROM {{ source('world_analysis_python', 'world_size_growth_slopes_historical') }}
INNER JOIN {{ ref('world_hyde_urbanization') }}
USING (country, year)
