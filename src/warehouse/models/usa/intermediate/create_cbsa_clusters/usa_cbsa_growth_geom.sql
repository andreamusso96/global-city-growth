{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
) }}

{% set crosswalk_table = source('matching', 'usa_crosswalk_component_id_to_cbsa_id') %}
{% set cluster_base_geom_table = ref('usa_cbsa_base_geom') %}

{{ create_cluster_growth_from_cluster_base(cluster_base_geom_table, crosswalk_table) }}
