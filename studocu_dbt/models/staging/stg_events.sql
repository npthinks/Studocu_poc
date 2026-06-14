{{ config(
    materialized='table',
    format='parquet',
    write_compression='snappy',
    partitioned_by=['event_date']
) }}

with source as (
    select * from {{ source('studocu_poc', 'silver') }}
),

renamed as (
    select
        event_id,
        event_timestamp,
        event_type,
        user_id,
        session_id,
        document_id,
        university_id,
        country_code,
        device_type,
        page_url,
        referrer,
        duration_seconds,
        is_premium_user,
        ingestion_timestamp,
        cast(event_date as date) as event_date
    from source
)

select * from renamed