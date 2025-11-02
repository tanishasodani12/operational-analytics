USE `operation_&_metric_analytics`;

# Case Study 1: Job Data Analysis

-- A : Jobs Reviewed Over Time
SELECT 
    STR_TO_DATE(ds, '%m/%d/%Y') AS review_date,
    COUNT(*) AS jobs_reviewed
FROM 
    job_data
WHERE 
    event = 'decision'
    AND STR_TO_DATE(ds, '%m/%d/%Y') BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY 
    STR_TO_DATE(ds, '%m/%d/%Y')
ORDER BY 
    review_date;
    
-- B : Throughput Analysis
WITH daily_events AS (
    SELECT 
        STR_TO_DATE(ds, '%m/%d/%Y') AS review_date,
        COUNT(*) AS total_events
    FROM job_data
    GROUP BY ds
)
SELECT
    review_date,
    total_events,
    FORMAT(total_events / 86400.0, 6) AS daily_throughput,
    FORMAT(
        AVG(total_events) OVER (
            ORDER BY review_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) / 86400.0, 6
    ) AS rolling_avg_throughput
FROM daily_events
ORDER BY review_date;



-- C : Language Share Analysis
WITH recent_jobs AS (
    SELECT * 
    FROM job_data
    WHERE STR_TO_DATE(ds, '%m/%d/%Y') >= (
        SELECT DATE_SUB(MAX(STR_TO_DATE(ds, '%m/%d/%Y')), INTERVAL 30 DAY)
        FROM job_data
    )
),
lang_usage AS (
    SELECT language, COUNT(*) AS lang_count
    FROM recent_jobs
    GROUP BY language
),
total_usage AS (
    SELECT COUNT(*) AS total_count FROM recent_jobs
)
SELECT
    l.language,
    l.lang_count,
    ROUND((l.lang_count * 100.0 / t.total_count), 2) AS percentage_share
FROM
    lang_usage l
CROSS JOIN
    total_usage t
ORDER BY
    percentage_share DESC;


-- D : Duplicate Rows Detection
SELECT 
  job_id, actor_id, event, language, time_spent, org, ds,
  COUNT(*) AS row_count
FROM job_data
GROUP BY 
	job_id, actor_id, event, language, time_spent, org, ds
HAVING COUNT(*) > 1;