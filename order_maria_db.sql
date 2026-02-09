WITH ordered_data AS (
  SELECT 
    id,
    routing_sequence,
    timestamp_column,
    LAG(timestamp_column) OVER (ORDER BY routing_sequence) AS prev_timestamp
  FROM your_table
)
SELECT 
  id,
  routing_sequence,
  timestamp_column,
  prev_timestamp,
  CASE 
    WHEN prev_timestamp IS NULL THEN 'First record'
    WHEN timestamp_column >= prev_timestamp THEN 'In order'
    ELSE 'Out of order'
  END AS timestamp_order_status,
  TIMESTAMPDIFF(MICROSECOND, prev_timestamp, timestamp_column) AS time_diff_microseconds,
  TIMESTAMPDIFF(SECOND, prev_timestamp, timestamp_column) AS time_diff_seconds
FROM ordered_data
ORDER BY routing_sequence;
