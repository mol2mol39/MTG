-- デッキカラー、ターン、ユーザーのランクごとの勝敗数を取得する。
-- ターン数が4以下は４、１５以上は１５に統一する（データ数が少ないので）
select
  d.main_colors,
  case 
    when g.num_turns >= 15 then 15
    when g.num_turns <= 4 then 4
    else g.num_turns 
  end as num_turns,
  g.won,
  u.rank,
  count(*)
from
  woe.game g
inner join
  woe.deck d
on
  g.draft_id = d.draft_id
  and g.build_index = d.build_index
inner join
  woe.user u
on
  g.draft_id = u.draft_id
group by
  d.main_colors,
  g.won,
  u.rank,
  case
    when g.num_turns >= 15 then 15
    when g.num_turns <= 4 then 4
    else g.num_turns
  end
order by d.main_colors, num_turns, g.won, u.rank