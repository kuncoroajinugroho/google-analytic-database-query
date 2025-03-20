

WITH dim_user AS (
  SELECT id
  FROM `dwh_prod`.`dim_users`
  WHERE verified_id = 1
  GROUP BY 1
),

user_carstensz AS (
  SELECT id
  FROM `dwh_prod_carstensz_user_source`.`carstensz_user_users_source`
  GROUP BY 1
),

event_mapper AS (
  SELECT
    event_name_mapper_id_sk,
    product_owner,
    event_name,
    section,
    metric,
    product_name,
    source_name,
    mapper_group
  FROM `privydata`.`dwh_prod`.`dim_ga_event_name_mapper`
),

backup_data AS (
  SELECT
    event_id_sk,
    event_date,
    operating_system,
    event_time,
    user_pseudo_id,
    metric,
    user_id,
    ga_session_id,
    created_date,
    event_number
  FROM `dwh_prod_google_analytics_source`.`ga_backup_mobile_raw_events_source`
  WHERE event_number = 1
    
      AND
      created_date BETWEEN DATE_ADD(
        '2100-01-01', INTERVAL -5 DAY
      ) AND '2100-01-01'
    
),

mobile AS (
  SELECT
    event_id_sk,
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
    event_number
  FROM `dwh_prod_google_analytics_source`.`ga_mobile_raw_events_source`
  WHERE
    1 = 1
    AND event_group_name IN ("item_click", "action", "screen_view", "event_name")
    AND event_number = 1
    
      AND
      created_date BETWEEN DATE_ADD(
        '2100-01-01', INTERVAL -5 DAY
      ) AND '2100-01-01'
    
),

new_mobile AS (
  SELECT
    event_id_sk,
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
    event_number
  FROM `dwh_prod_google_analytics_source`.`ga_new_mobile_raw_events_source`
  WHERE
    1 = 1
    AND event_group_name IN ("item_click", "action", "screen_view", "event_name")
    AND event_number = 1
    
      AND
      created_date BETWEEN DATE_ADD(
        '2100-01-01', INTERVAL -5 DAY
      ) AND '2100-01-01'
    
),

user_id AS (
  SELECT id
  FROM dim_user
  UNION DISTINCT
  SELECT id
  FROM user_carstensz
),

prep_backup AS (
  SELECT
    backup_data.event_id_sk              AS event_id_sk,
    backup_data.event_date               AS event_date,
    backup_data.event_time               AS event_timestamp,
    event_mapper.event_name              AS event_name,
    backup_data.metric                   AS metric,
    event_mapper.product_owner           AS product_owner,
    event_mapper.section                 AS section,
    event_mapper.product_name            AS product_name,
    event_mapper.source_name             AS source_event_name,
    backup_data.ga_session_id            AS ga_session_id,
    backup_data.user_id                  AS user_id,
    backup_data.user_pseudo_id           AS user_pseudo_id,
    backup_data.operating_system         AS device_operating_system,
    FALSE                                AS is_active_user,
    "old"                                AS source_database,
    backup_data.created_date             AS created_date,
    event_mapper.event_name_mapper_id_sk AS event_mapper_id,
    CAST("" AS STRING)                   AS metric_type,
    CAST(NULL AS INTEGER)                AS user_level,
    CAST("" AS STRING)                   AS payment_product,
    CAST("" AS STRING)                   AS payment_group,
    CAST("" AS STRING)                   AS payment_channel,
    CAST("" AS STRING)                   AS platform,
    CAST("" AS STRING)                   AS device_category,
    CAST("" AS STRING)                   AS device_mobile_brand_name,
    CAST("" AS STRING)                   AS device_mobile_model_name,
    CAST("" AS STRING)                   AS device_mobile_marketing_name,
    CAST("" AS STRING)                   AS device_mobile_os_hardware_model,
    CAST("" AS STRING)                   AS device_operating_system_version,
    CAST("" AS STRING)                   AS device_language,
    CAST("" AS STRING)                   AS geo_city,
    CAST("" AS STRING)                   AS geo_country,
    CAST("" AS STRING)                   AS geo_continent,
    CAST("" AS STRING)                   AS geo_region,
    CAST("" AS STRING)                   AS geo_sub_continent,
    CAST("" AS STRING)                   AS app_info_version,
    CAST("" AS STRING)                   AS traffic_source_name,
    CAST("" AS STRING)                   AS traffic_source_medium,
    CAST("" AS STRING)                   AS traffic_source_source
  FROM backup_data
  LEFT JOIN user_id ON backup_data.user_id = user_id.id
  LEFT JOIN event_mapper ON backup_data.metric = event_mapper.metric
),

prep_mobile AS (
  SELECT
    mobile.event_id_sk                     AS event_id_sk,
    mobile.event_date                      AS event_date,
    mobile.event_timestamp                 AS event_timestamp,
    mobile.event_name                      AS event_name,
    mobile.metric                          AS metric,
    mobile.metric_type                     AS metric_type,
    event_mapper.product_owner             AS product_owner,
    event_mapper.section                   AS section,
    event_mapper.product_name              AS product_name,
    event_mapper.source_name               AS source_event_name,
    mobile.ga_session_id                   AS ga_session_id,
    mobile.user_id                         AS user_id,
    mobile.user_pseudo_id                  AS user_pseudo_id,
    mobile.user_level                      AS user_level,
    mobile.payment_product                 AS payment_product,
    mobile.payment_group                   AS payment_group,
    mobile.payment_channel                 AS payment_channel,
    mobile.platform                        AS platform,
    mobile.device_category                 AS device_category,
    mobile.device_mobile_brand_name        AS device_mobile_brand_name,
    mobile.device_mobile_model_name        AS device_mobile_model_name,
    mobile.device_mobile_marketing_name    AS device_mobile_marketing_name,
    mobile.device_mobile_os_hardware_model AS device_mobile_os_hardware_model,
    mobile.device_operating_system         AS device_operating_system,
    mobile.device_operating_system_version AS device_operating_system_version,
    mobile.device_language                 AS device_language,
    mobile.geo_city                        AS geo_city,
    mobile.geo_country                     AS geo_country,
    mobile.geo_continent                   AS geo_continent,
    mobile.geo_region                      AS geo_region,
    mobile.geo_sub_continent               AS geo_sub_continent,
    mobile.app_info_version                AS app_info_version,
    mobile.traffic_source_name             AS traffic_source_name,
    mobile.traffic_source_medium           AS traffic_source_medium,
    mobile.traffic_source_source           AS traffic_source_source,
    mobile.is_active_user                  AS is_active_user,
    "new"                                  AS source_database,
    mobile.created_date                    AS created_date,
    event_mapper.event_name_mapper_id_sk   AS event_mapper_id
  FROM mobile
  LEFT JOIN user_id ON mobile.user_id = user_id.id
  LEFT JOIN event_mapper ON mobile.metric = event_mapper.metric
),

prep_new_mobile AS (
  SELECT
    new_mobile.event_id_sk                     AS event_id_sk,
    new_mobile.event_date                      AS event_date,
    new_mobile.event_timestamp                 AS event_timestamp,
    new_mobile.event_name                      AS event_name,
    new_mobile.metric                          AS metric,
    new_mobile.metric_type                     AS metric_type,
    event_mapper.product_owner                 AS product_owner,
    event_mapper.section                       AS section,
    event_mapper.product_name                  AS product_name,
    event_mapper.source_name                   AS source_event_name,
    new_mobile.ga_session_id                   AS ga_session_id,
    new_mobile.user_id                         AS user_id,
    new_mobile.user_pseudo_id                  AS user_pseudo_id,
    new_mobile.user_level                      AS user_level,
    new_mobile.payment_product                 AS payment_product,
    new_mobile.payment_group                   AS payment_group,
    new_mobile.payment_channel                 AS payment_channel,
    new_mobile.platform                        AS platform,
    new_mobile.device_category                 AS device_category,
    new_mobile.device_mobile_brand_name        AS device_mobile_brand_name,
    new_mobile.device_mobile_model_name        AS device_mobile_model_name,
    new_mobile.device_mobile_marketing_name    AS device_mobile_marketing_name,
    new_mobile.device_mobile_os_hardware_model AS device_mobile_os_hardware_model,
    new_mobile.device_operating_system         AS device_operating_system,
    new_mobile.device_operating_system_version AS device_operating_system_version,
    new_mobile.device_language                 AS device_language,
    new_mobile.geo_city                        AS geo_city,
    new_mobile.geo_country                     AS geo_country,
    new_mobile.geo_continent                   AS geo_continent,
    new_mobile.geo_region                      AS geo_region,
    new_mobile.geo_sub_continent               AS geo_sub_continent,
    new_mobile.app_info_version                AS app_info_version,
    new_mobile.traffic_source_name             AS traffic_source_name,
    new_mobile.traffic_source_medium           AS traffic_source_medium,
    new_mobile.traffic_source_source           AS traffic_source_source,
    new_mobile.is_active_user                  AS is_active_user,
    "new_privysign"                            AS source_database,
    new_mobile.created_date                    AS created_date,
    event_mapper.event_name_mapper_id_sk       AS event_mapper_id
  FROM new_mobile
  LEFT JOIN user_id ON new_mobile.user_id = user_id.id
  LEFT JOIN event_mapper ON new_mobile.metric = event_mapper.metric
),

output_backup AS (
  SELECT
        to_hex(md5(cast(coalesce(cast(event_id_sk as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_database as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_mapper_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS event_mobile_id_sk,
    event_date,
    event_timestamp,
    event_name,
    metric,
    metric_type,
    product_owner,
    section,
    product_name,
    source_event_name,
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
    source_database,
    created_date
  FROM prep_backup
),

output_mobile AS (
  SELECT
        to_hex(md5(cast(coalesce(cast(event_id_sk as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_database as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_mapper_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS event_mobile_id_sk,
    event_date,
    event_timestamp,
    event_name,
    metric,
    metric_type,
    product_owner,
    section,
    product_name,
    source_event_name,
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
    source_database,
    created_date
  FROM prep_mobile
),

output_new_mobile AS (
  SELECT
        to_hex(md5(cast(coalesce(cast(event_id_sk as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_database as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_mapper_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS event_mobile_id_sk,
    event_date,
    event_timestamp,
    event_name,
    metric,
    metric_type,
    product_owner,
    section,
    product_name,
    source_event_name,
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
    source_database,
    created_date
  FROM prep_new_mobile
),

output AS (
  SELECT * FROM output_backup
  UNION ALL
  SELECT * FROM output_mobile
  UNION ALL
  SELECT * FROM output_new_mobile
)

SELECT
      *
    FROM output
