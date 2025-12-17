{{
    config(
        materialized='incremental',
        unique_key='edge_id' 
    )
}}

{{ dbt_id_stitching.make_edges() }}