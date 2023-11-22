import json
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from postgresql.interface import PostgresInterface

output_dir = os.path.join(
    os.path.dirname(__file__),
    '../../data/output'
)
os.makedirs(output_dir, exist_ok=True)

db = PostgresInterface('WOE')

# 2colorヒートマップ作成用データ出力
data = db.select_game_data('dict')

with open(os.path.join(output_dir, '2color_winning_rate.json'), 'w') as file:
    json.dump(data, file, separators=(',', ':'))
