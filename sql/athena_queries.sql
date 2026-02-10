-- =========================================================
-- AWS Call Center Analytics – Athena Queries
-- Database: callcenter_analytics
-- =========================================================

-- 1️⃣ Daily Call Summary (latest days first)
SELECT
    call_date,
    total_calls,
    completed_calls,
    dropped_calls,
    failed_calls,
    completion_rate
FROM callcenter_analytics.gold_call_daily_summary
ORDER BY call_date DESC
LIMIT 10;


-- 2️⃣ Agent Performance Overview
SELECT
    agent_id,
    call_date,
    total_calls,
    completed_calls,
    dropped_calls,
    failed_calls,
    completion_rate
FROM callcenter_analytics.gold_agent_performance
ORDER BY call_date DESC, agent_id
LIMIT 20;


-- 3️⃣ Daily Call Duration Metrics
SELECT
    call_date,
    avg_call_duration_seconds,
    min_call_duration_seconds,
    max_call_duration_seconds
FROM callcenter_analytics.gold_call_duration_metrics
ORDER BY call_date DESC
LIMIT 10;


-- 4️⃣ Top Performing Agents (by completion rate)
SELECT
    agent_id,
    AVG(completion_rate) AS avg_completion_rate
FROM callcenter_analytics.gold_agent_performance
GROUP BY agent_id
ORDER BY avg_completion_rate DESC
LIMIT 5;


-- 5️⃣ Identify Days with High Call Failures
SELECT
    call_date,
    failed_calls,
    total_calls,
    ROUND((failed_calls * 100.0) / total_calls, 2) AS failure_rate_pct
FROM callcenter_analytics.gold_call_daily_summary
WHERE total_calls > 0
ORDER BY failure_rate_pct DESC
LIMIT 10;
