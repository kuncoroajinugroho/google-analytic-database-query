

WITH source AS (
  SELECT
    *,
    PARSE_DATE('%Y%m%d', `_TABLE_SUFFIX`) AS created_date
  FROM `privydata`.`google_analytics`.`mobile_raw_events_*`
),

renamed AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date)                AS event_date,
    CAST(event_name AS STRING)                      AS event_name,
    DATETIME(
      TIMESTAMP_SECONDS(
        CAST(CAST(event_timestamp AS INT64) / 1000000 AS INT64)
      ), 'Asia/Jakarta'
    )                                               AS event_timestamp,
    CASE
      WHEN
        event_name IN ('item_click')
        THEN (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'view_name'
        )
      WHEN
        event_name IN ('action')
        THEN (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'action_name'
        )
      WHEN
        event_name IN ('screen_view')
        THEN (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'firebase_screen_class'
        )
      WHEN
        event_name NOT IN ('item_click', 'screen_view', 'action')
        THEN event_name
    END                                             AS metric,
    CASE
      WHEN
        event_name IN ('item_click')
        THEN (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'view_type'
        )
      WHEN
        event_name IN ('action')
        THEN (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'action_type'
        )
      WHEN
        event_name IN ('screen_view')
        AND user_id IS NOT NULL THEN ''
      WHEN
        event_name NOT IN ('item_click', 'screen_view', 'action')
        THEN ''
    END                                             AS metric_type,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    )                                               AS ga_session_id,
    CAST(user_id AS STRING)                         AS user_id,
    CAST(user_pseudo_id AS STRING)                  AS user_pseudo_id,
    CAST((
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'user_level'
    ) AS INTEGER)                                   AS user_level,
    CAST((
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'payment_product'
    ) AS STRING)                                    AS payment_product,
    CAST((
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'group_name'
    ) AS STRING)                                    AS payment_group,
    CAST((
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'channel_name'
    ) AS STRING)                                    AS payment_channel,
    CAST(platform AS STRING)                        AS platform,
    CAST(device.category AS STRING)                 AS device_category,
    CAST(device.mobile_brand_name AS STRING)        AS device_mobile_brand_name,
    CAST(device.mobile_model_name AS STRING)        AS device_mobile_model_name,
    CAST(device.mobile_marketing_name AS STRING)    AS device_mobile_marketing_name,
    CAST(device.mobile_os_hardware_model AS STRING) AS device_mobile_os_hardware_model,
    CAST(device.operating_system AS STRING)         AS device_operating_system,
    CAST(device.operating_system_version AS STRING) AS device_operating_system_version,
    CAST(device.language AS STRING)                 AS device_language,
    CAST(geo.city AS STRING)                        AS geo_city,
    CAST(geo.country AS STRING)                     AS geo_country,
    CAST(geo.continent AS STRING)                   AS geo_continent,
    CAST(geo.region AS STRING)                      AS geo_region,
    CAST(geo.sub_continent AS STRING)               AS geo_sub_continent,
    CAST(app_info.version AS STRING)                AS app_info_version,
    CAST(traffic_source.name AS STRING)             AS traffic_source_name,
    CAST(traffic_source.medium AS STRING)           AS traffic_source_medium,
    CAST(traffic_source.source AS STRING)           AS traffic_source_source,
    CAST(is_active_user AS BOOLEAN)                 AS is_active_user,
    DATE(created_date)                              AS created_date,
    CASE
      WHEN
        event_name IN ('item_click')
        THEN 'item_click'
      WHEN
        event_name IN ('action')
        THEN 'action'
      WHEN
        event_name IN ('screen_view')
        THEN 'screen_view'
      WHEN
        event_name NOT IN ('item_click', 'screen_view', 'action')
        THEN 'event_name'
    END                                             AS event_group_name
  FROM source
  
    WHERE
      created_date BETWEEN DATE_ADD(
        '2100-01-01', INTERVAL -5 DAY
      ) AND '2100-01-01'
  
),

output AS (
  SELECT
        to_hex(md5(cast(coalesce(cast(event_date as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_pseudo_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ga_session_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(metric as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(metric_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_name as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_timestamp as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ROW_NUMBER() OVER(PARTITION BY user_id, user_pseudo_id, ga_session_id, metric, metric_type, event_name, event_timestamp) as string), '_dbt_utils_surrogate_key_null_') as string))) AS event_id_sk,
    event_date,
    event_name,
    event_timestamp,
    metric,
    metric_type,
    ga_session_id,
    user_id,
    user_pseudo_id,
    user_level,
    payment_product,
    payment_group,
    payment_channel,
    platform,
    device_category,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_mobile_marketing_name,
    device_mobile_os_hardware_model,
    device_operating_system,
    device_operating_system_version,
    device_language,
    geo_city,
    geo_country,
    geo_continent,
    geo_region,
    geo_sub_continent,
    app_info_version,
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source,
    is_active_user,
    event_group_name,
    created_date,
    ROW_NUMBER()
      OVER (
        PARTITION BY
          event_date,
          user_id,
          user_pseudo_id,
          ga_session_id,
          metric,
          metric_type,
          event_name,
          event_timestamp
      )
      AS event_number
  FROM renamed
)

SELECT
      *,
    FROM output