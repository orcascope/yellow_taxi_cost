--Get run_id by the job_parameter
SELECT
  sku_name,
  billing_origin_product,
  usage_date,
  SUM(usage_quantity) AS `DBUs`,
  elapsed_run_minutes
FROM
  system.billing.usage u
  join (select run_id, timestampdiff(MINUTE, period_start_time, period_end_time) as elapsed_run_minutes,
        job_parameters['PROFILE']['value'] as run_name
        from system.lakeflow.job_run_timeline) jrl on jrl.run_id  = u.usage_metadata.job_run_id
WHERE
  run_name in ('batch_aggr')
GROUP BY ALL;