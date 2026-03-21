{{ config(
    indexes=[
      {'columns': ['cluster_id', 'y1', 'y2']},
      {'columns': ['neighbor_id', 'y1', 'y2']}
    ]
) }}

{% set years = var('constants')['GHSL_RASTER_YEARS'] %}

WITH candidate_neighbors AS (
    {% set queries = [] %}
    {% for y1 in years %}
        {% set y2 = y1 + 10 %}
        {% if y2 in years %}
            {% set query %}
            SELECT  s.cluster_id,
                    n.cluster_id AS neighbor_id,
                    s.country,
                    s.y1,
                    s.y2,
                    ST_Distance(s.geom, n.geom) AS distance,
                    s.area,
                    s.population_y1,
                    s.population_y2,
                    n.population_y1 AS neighbor_population_y1,
                    n.population_y2 AS neighbor_population_y2
            FROM {{ ref('world_suburbanization_augmented_cluster_metrics') }} s
            INNER JOIN {{ ref('world_suburbanization_augmented_cluster_metrics') }} n
            ON s.country = n.country
            AND n.centroid && ST_Expand(s.centroid, 100000)
            AND ST_DWithin(s.centroid, n.centroid, 100000)
            WHERE s.y1 = {{ y1 }}
            AND s.y2 = {{ y2 }}
            AND n.y1 = {{ y1 }}
            AND n.y2 = {{ y2 }}
            {% endset %}
            {% do queries.append(query) %}
        {% endif %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT  cluster_id,
        neighbor_id,
        country,
        y1,
        y2,
        distance,
        area,
        population_y1,
        population_y2,
        neighbor_population_y1,
        neighbor_population_y2
FROM candidate_neighbors
