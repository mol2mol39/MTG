-- deck_id, expansionを渡す

select distinct c.card_name, c.num, cm.rarity, cm.mana_value, cm.types
from woe.cards c
left join public.card_master cm 
  on c.card_name = cm.card_name
where c.id = %s
and cm.expansion in (%s, 'PUBLIC')
order by c.card_name;
