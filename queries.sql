-- 1. Event trend (daily activity)
SELECT DATE(event_ts) AS dt, COUNT(*) AS total_events
FROM raw_events
GROUP BY DATE(event_ts)
ORDER BY dt;

-- 2. Top users by activity
SELECT user_login, COUNT(*) AS event_count
FROM raw_events
GROUP BY user_login
ORDER BY event_count DESC
LIMIT 10;

-- 3. Audience counts (validation)
SELECT 
    (SELECT COUNT(*) FROM aud_high_intent_users) AS high_intent_count,
    (SELECT COUNT(*) FROM aud_newly_engaged_users) AS new_users_count;

-- 4. Suppression validation (should ideally be 0)
SELECT COUNT(*) AS suppressed_in_audience
FROM aud_high_intent_users
WHERE user_login IN (
    SELECT user_login FROM suppression_list
);

-- 5. Event type distribution
SELECT event_type, COUNT(*) AS event_count
FROM raw_events
GROUP BY event_type
ORDER BY event_count DESC;
