-- イベントでカラーが違うデッキを使用していたもの
select g.draft_id, g.build_index, g.match_number, d.main_colors, d.splash_colors, g.won
from woe.game g 
left join woe.deck d
ON g.draft_id = d.draft_id AND g.build_index = d.build_index
where g.draft_id in (
	SELECT draft_id
	FROM woe.deck
	GROUP BY draft_id
	HAVING COUNT(DISTINCT main_colors) > 1
)
order by g.draft_id, g.match_number;

-- 各イベントの使用デッキの最頻値を取得する。同数の場合は最後に使用していたものを取得
SELECT 
  draft_id,
  MODE() WITHIN GROUP (ORDER BY build_index DESC) AS consolidated_build_index
FROM
  woe.game
GROUP BY
  draft_id;


-- draft_idごとのデッキカラー取得
WITH cte AS (
  SELECT
    draft_id,
    MODE() WITHIN GROUP (ORDER BY build_index DESC) AS consolidated_build_index
  FROM woe.game
  GROUP BY draft_id
)
SELECT
  cte.draft_id,
  cte.consolidated_build_index,
  d.main_colors,
  u.rank
FROM cte
JOIN woe.deck d
  ON cte.draft_id = d.draft_id
  AND cte.consolidated_build_index = d.build_index
JOIN woe.user u
  ON cte.draft_id = u.draft_id
order by cte.draft_id;


SELECT
  SUM(CASE WHEN won = TRUE THEN 1 ELSE 0 END) AS true_count,
  SUM(CASE WHEN won = FALSE THEN 1 ELSE 0 END) AS false_count
from woe.game
where draft_id = '0652a2b94c24455392528fdfb778f672';