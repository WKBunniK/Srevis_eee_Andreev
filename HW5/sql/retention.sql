WITH first_views AS
(
    SELECT user_id, min(toDate(timestamp)) AS cohort_date
    FROM cinema.events FINAL
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY user_id
    HAVING cohort_date = {day:Date}
), activity AS
(
    SELECT DISTINCT user_id, toDate(timestamp) AS activity_date
    FROM cinema.events FINAL
    WHERE toDate(timestamp) BETWEEN {day:Date} AND {day:Date} + 7
)
SELECT
    uniqExact(first_views.user_id) AS cohort_size,
    uniqExactIf(first_views.user_id, activity.activity_date = {day:Date} + 1) AS returned_d1,
    uniqExactIf(first_views.user_id, activity.activity_date = {day:Date} + 7) AS returned_d7
FROM first_views INNER JOIN activity ON first_views.user_id = activity.user_id
