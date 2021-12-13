
      

  
    create table test_normalization.nested_stream_with_complex_columns_resulting_into_long_names_scd
    
  
    
    engine = MergeTree()
    
    order by (tuple())
    
  as (
    
-- depends_on: ref('nested_stream_with_complex_columns_resulting_into_long_names_stg')
with

input_data as (
    select *
    from _airbyte_test_normalization.nested_stream_with_complex_columns_resulting_into_long_names_stg
    -- nested_stream_with_complex_columns_resulting_into_long_names from test_normalization._airbyte_raw_nested_stream_with_complex_columns_resulting_into_long_names
),

input_data_with_active_row_num as (
    select *,
      row_number() over (
        partition by id
        order by
            date is null asc,
            date desc,
            _airbyte_emitted_at desc
      ) as _airbyte_active_row_num
    from input_data
),
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      assumeNotNull(hex(MD5(
            
                toString(id)
            
    ))) as _airbyte_unique_key,
        id,
        date,
        partition,
      date as _airbyte_start_at,
      anyOrNull(date) over (
        partition by id
        order by
            date is null asc,
            date desc,
            _airbyte_emitted_at desc
        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
      ) as _airbyte_end_at,
      case when _airbyte_active_row_num = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_nested_stream_with_complex_columns_resulting_into_long_names_hashid
    from input_data_with_active_row_num
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by _airbyte_unique_key, _airbyte_start_at, _airbyte_emitted_at
            order by _airbyte_active_row desc, _airbyte_ab_id
        ) as _airbyte_row_num,
        assumeNotNull(hex(MD5(
            
                toString(_airbyte_unique_key) || '~' ||
            
            
                toString(_airbyte_start_at) || '~' ||
            
            
                toString(_airbyte_emitted_at)
            
    ))) as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
        id,
        date,
        partition,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_nested_stream_with_complex_columns_resulting_into_long_names_hashid
from dedup_data where _airbyte_row_num = 1
  )
  