SELECT
    count() AS processed_events,
    uniqExact(user_id) AS dau,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS view_conversion
FROM cinema.events FINAL
WHERE toDate(timestamp) = {day:Date}
