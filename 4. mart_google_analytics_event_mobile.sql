

WITH event_mobile AS (
  SELECT
    event_mobile_id_sk,
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
  FROM `privydata`.`dwh_prod`.`fct_ga_event_mobile`
  WHERE
    1 = 1
    
      AND
      created_date BETWEEN DATE_ADD(
        '2100-01-01', INTERVAL -5 DAY
      ) AND '2100-01-01'
    
),

prep AS (
  SELECT
    event_mobile.event_mobile_id_sk              AS event_mobile_id,
    event_mobile.event_date                      AS event_date,
    event_mobile.event_timestamp                 AS event_timestamp,
    event_mobile.event_name                      AS event_name,
    event_mobile.metric                          AS metric,
    event_mobile.metric_type                     AS metric_type,
    event_mobile.product_owner                   AS product_owner,
    event_mobile.section                         AS section,
    event_mobile.product_name                    AS product_name,
    event_mobile.source_event_name               AS source_event_name,
    event_mobile.ga_session_id                   AS ga_session_id,
    event_mobile.user_id                         AS user_id,
    event_mobile.user_pseudo_id                  AS user_pseudo_id,
    event_mobile.user_level                      AS user_level,
    event_mobile.payment_product                 AS payment_product,
    event_mobile.payment_group                   AS payment_group,
    event_mobile.payment_channel                 AS payment_channel,
    event_mobile.platform                        AS platform,
    event_mobile.device_category                 AS device_category,
    event_mobile.device_mobile_brand_name        AS device_mobile_brand_name,
    event_mobile.device_mobile_model_name        AS device_mobile_model_name,
    event_mobile.device_mobile_marketing_name    AS device_mobile_marketing_name,
    event_mobile.device_mobile_os_hardware_model AS device_mobile_os_hardware_model,
    event_mobile.device_operating_system         AS device_operating_system,
    event_mobile.device_operating_system_version AS device_operating_system_version,
    event_mobile.device_language                 AS device_language,
    event_mobile.geo_city                        AS geo_city,
    event_mobile.geo_country                     AS geo_country,
    event_mobile.geo_continent                   AS geo_continent,
    event_mobile.geo_region                      AS geo_region,
    event_mobile.geo_sub_continent               AS geo_sub_continent,
    event_mobile.app_info_version                AS app_info_version,
    event_mobile.traffic_source_name             AS traffic_source_name,
    event_mobile.traffic_source_medium           AS traffic_source_medium,
    event_mobile.traffic_source_source           AS traffic_source_source,
    event_mobile.is_active_user                  AS is_active_user,
    event_mobile.source_database                 AS source_database,
    event_mobile.created_date                    AS created_date
  FROM event_mobile
),

output AS (
  SELECT
        to_hex(md5(cast(coalesce(cast(event_mobile_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('mobile' as string), '_dbt_utils_surrogate_key_null_') as string))) AS event_mobile_id_sk,
    event_mobile_id,
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
  FROM prep
)

SELECT
      *,
      CAST('kuncoro' AS STRING)     AS created_by,
      CAST('kuncoro' AS STRING)     AS updated_by,
      DATE('2024-07-17')             AS model_created_date,
      DATE('2024-08-30')             AS model_updated_date,
      CURRENT_DATETIME('Asia/Jakarta')       AS dbt_audit_updated_at,

      DATETIME('2024-11-25 13:15:10.973699') AS dbt_audit_created_at

    FROM output