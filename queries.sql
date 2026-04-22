-- 1. Event trend
SELECT DATE(event_ts) as dt, COUNT(*) as total_events
FROM raw_events
GROUP BY dt
ORDER BY dt;

-- 2. Top users
SELECT user_login, COUNT(*) as event_count
FROM raw_events
GROUP BY user_login
ORDER BY event_count DESC
LIMIT 10;

-- 3. Audience counts
SELECT 
    (SELECT COUNT(*) FROM aud_high_intent_users) as high_intent_count,
    (SELECT COUNT(*) FROM aud_newly_engaged_users) as new_users_count;

-- 4. Suppression check (should be 0)
SELECT COUNT(*) as suppressed_in_audience
FROM aud_high_intent_users
WHERE user_login IN (SELECT user_login FROM suppression_list);

-- 5. Event type distribution
SELECT event_type, COUNT(*) as count
FROM raw_events
GROUP BY event_type
ORDER BY count DESC;
