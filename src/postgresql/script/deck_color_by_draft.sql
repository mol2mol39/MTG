-- ドラフトID単位でデッキカラーを取得する
-- 複数のデッキカラーがある場合は最頻値を取得、同数の場合は後半に使用していたデッキを取得
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
  d.deck_id,
  u.rank
FROM cte
JOIN woe.deck d
  ON cte.draft_id = d.draft_id
  AND cte.consolidated_build_index = d.build_index
JOIN woe.user u
  ON cte.draft_id = u.draft_id
order by cte.draft_id;