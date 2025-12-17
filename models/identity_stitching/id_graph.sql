{{
    config(
        materialized='table'
    )
}}

{{ dbt_id_stitching.make_id_graph(edge_table=ref('edges')) }}