SELECT
  SUM(CASE WHEN won = TRUE THEN 1 ELSE 0 END) AS true_count,
  SUM(CASE WHEN won = FALSE THEN 1 ELSE 0 END) AS false_count
from woe.game
where draft_id = %s;
