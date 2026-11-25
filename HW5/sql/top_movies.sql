SELECT movie_id, count() AS views,
       row_number() OVER (ORDER BY count() DESC, movie_id) AS rank
FROM cinema.events FINAL
WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY rank
