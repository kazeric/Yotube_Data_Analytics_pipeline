

--- Likes and Comments Over Time for a newly posted video
SELECT
  extraction_time AS "time",
  video_id,
  like_count,
  comment_count
FROM video_stats
WHERE video_id = 'k64goYZL69E'
ORDER BY time;


--- Top Videos by Views
SELECT
  video_id,
  title,
  (MAX(view_count)/100) AS max_views_h,
  MAX(like_count) AS max_likes,
  MAX(comment_count) AS max_comments
FROM video_stats
GROUP BY video_id, title
ORDER BY max_views_h DESC
LIMIT 10;

--- Engagement Rate Trend
SELECT
  DATE_TRUNC('hour', extraction_time) AS "time",
  AVG(like_count::float / NULLIF(view_count, 0)) AS avg_like_rate,
  AVG(comment_count::float / NULLIF(view_count, 0)) AS avg_comment_rate
FROM video_stats
GROUP BY DATE_TRUNC('hour', extraction_time)
ORDER BY "time";