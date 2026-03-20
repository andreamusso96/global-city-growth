WITH min_log_population AS (
    SELECT  definition,
            year,
            MIN(log_population) AS min_log_population
    FROM {{ ref('usa_suburbanization_size_vs_growth') }}
    GROUP BY definition, year
),
size_vs_growth_normalized AS (
    SELECT  sg.definition,
            sg.cluster_id,
            sg.year,
            sg.log_population,
            sg.log_growth,
            sg.log_population - mlp.min_log_population AS normalized_log_population,
            sg.log_growth - ag.log_average_growth AS normalized_log_growth
    FROM {{ ref('usa_suburbanization_size_vs_growth') }} sg
    INNER JOIN min_log_population mlp
    USING (definition, year)
    INNER JOIN {{ ref('usa_suburbanization_average_growth') }} ag
    USING (definition, year)
)
SELECT *
FROM size_vs_growth_normalized
