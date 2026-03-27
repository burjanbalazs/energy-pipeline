with stats as (
                select
                    country,
                    activity_date,
                    avg_load_mw,
                    avg(avg_load_mw) over (partition by country) as mean_load,
                    stddev(avg_load_mw) over (partition by country) as stddev_load
                from {{ref('mart_daily_demand_summary') }}
            )

select
    *,
    abs(avg_load_mw - mean_load) / stddev_load as deviation_score
from stats
where abs(avg_load_mw - mean_load) > 2 * stddev_load