import json
from multiprocessing import Pool
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from postgresql.interface import PostgresInterface

RANK_LIST = ['bronze', 'silver', 'gold', 'diamond', 'platinum', 'mythic', 'unknown']

output_dir = os.path.join(
    os.path.dirname(__file__),
    '../../data/output'
)
os.makedirs(output_dir, exist_ok=True)


class Db:
    def __init__(self, expansion):
        self.db = PostgresInterface(expansion)
        with open(self.get_script_path('deck_color_by_draft.sql'), 'r') as f:
            self.DECK_COLOR_BY_DRAFT = f.read()
        with open(self.get_script_path('select_won_by_turns.sql')) as f:
            self.SELECT_WON_BY_TURNS = f.read()
        with open(self.get_script_path('select_won_count_by_draft.sql'), 'r') as f:
            self.SELECT_EVENT_RESULT = f.read()
        with open(self.get_script_path('select_deck_list.sql'), 'r') as f:
            self.SELECT_DECK_LIST = f.read()
        self.expansion = expansion

    def get_script_path(self, filename):
        return os.path.join(os.path.dirname(__file__), f"../postgresql/script/{filename}")

    # ドラフトIDごとのデッキ情報取得(event_winsのvalueの元データ)
    def select_deck_color_by_draft(self):
        df = self.db._exe_select_df(self.DECK_COLOR_BY_DRAFT)
        df['rank'].replace('', 'unknown', inplace=True)
        return df
    
    # デッキカラー、ランク、ターンごとの勝敗数取得（turn_infoのvalueの元データ）
    def select_won_by_turns(self):
        won_by_turns = self.db._exe_select_df(self.SELECT_WON_BY_TURNS)
        won_by_turns['rank'].replace('', 'unknown', inplace=True)
        return won_by_turns

    # ドラフトID単位（イベント単位）の勝敗数を取得する。勝: true_count, 敗: false_count
    # 戻り値例: { "true_count": 4, "false_count": 3 }
    def select_event_result(self, draft_id: str):
        event_result = self.db._exe_select_dict(self.SELECT_EVENT_RESULT, (draft_id,))
        return event_result[0]

    # デッキIDを指定してそのデッキのカードリストを取得する
    def select_deck_list(self, deck_id: str):
        deck_list = self.db._exe_select_df(self.SELECT_DECK_LIST, (deck_id, self.expansion, ))
        return deck_list


# デッキリストからそのデッキのマナカーブを取得する（７以上は７に統一する）
def get_mana_curve(deck_list):
     mana_curve_list = [0, 0, 0, 0, 0, 0, 0, 0]
     mana_curve = deck_list.groupby('mana_value')['num'].sum()
     for mana, count in mana_curve.items():
          index = mana if mana < 8 else 7
          mana_curve_list[index] = count
     return mana_curve_list


# イベント単位の成績（勝敗数）からデータの格納場所（リストのインデックス）を返す
def get_index_of_event_result(event_result):
    if event_result['true_count'] < 7 and event_result['false_count'] == 3:
        return event_result['true_count']
    elif event_result['true_count'] == 7 and event_result['false_count'] < 3:
        return 9 - event_result['false_count']
    else:
        return 10


# デッキカラーごとのturn_infoデータ作成処理
def get_won_by_turns_colors(deck_colors, won_by_turns_df):
    turn_info = {
        "won": [],
        "defeat": []
    }
    target_colors = won_by_turns_df[won_by_turns_df['main_colors'] == deck_colors]

    # 対象データがない場合は空で返す
    if len(target_colors) < 1:
        return turn_info
    
    # 決着ターン数ごとの処理
    for i in range(4, 16):
        won_by_turn = target_colors[target_colors['num_turns'] == i]
        for wd in ['won', 'defeat']:  # 勝ち、負けの数をそれぞれ取得する
            count_df = won_by_turn[won_by_turn['won'] == (wd == 'won')]

            count_dict = {}
            if len(count_df) > 0:
                count_dict = dict(zip(count_df['rank'], count_df['count']))
            
            count_dict['num_turns'] = i

            for r in RANK_LIST:
                if r not in count_dict:
                    count_dict[r] = 0

            turn_info[wd].append(count_dict)
    return turn_info


def init_result():
    result = []
    for i in range(11):
        tmp = {}
        for r in RANK_LIST:
            tmp[r] = {
                "event_count": 0,
                "mana_curve": [],
                "creature_mana_curve":[]
            }
        result.append(tmp)
    return result


# デッキ情報取得処理。並列処理で実行する想定
def worker(args):
    dbc, row = args
    # 戦績によってデッキ情報の格納場所を変える。戦績を取得して、格納場所のindexを計算する
    event_result = dbc.select_event_result(row['draft_id'])
    er_index = get_index_of_event_result(event_result)

    # デッキリストを取得してマナカーブの情報を計算する。クリーチャーのみのマナカーブも計算する
    deck_list = dbc.select_deck_list(row['deck_id'])
    mana_curve = get_mana_curve(deck_list)
    creatures = deck_list[deck_list['types'].str.contains('Creature')]
    creatures_mana_curve = get_mana_curve(creatures)
    return (er_index, row['rank'], mana_curve, creatures_mana_curve)


def main():
    dbc = Db('WOE')

    # 加工元データ取得
    deck_color_by_draft_df = dbc.select_deck_color_by_draft()
    won_by_turns_df = dbc.select_won_by_turns()

    json_data = {}
    grouped_colors_df = deck_color_by_draft_df.groupby('main_colors')

    # デッキカラー単位でデータ加工
    for deck_colors, group in grouped_colors_df:
        print(deck_colors, len(group))
  
        result = init_result()
        
        # デッキ情報取得処理。並列処理で実行
        pool = Pool(processes=os.cpu_count())
        pool_result = pool.map(worker, [(dbc, row) for _, row in group.iterrows()])
        pool.close()
        pool.join()
        
        for r in pool_result:
            er_index, rank, mana_curve, creatures_mana_curve = r
            result[er_index][rank]['mana_curve'].append(mana_curve)
            result[er_index][rank]['creature_mana_curve'].append(creatures_mana_curve)
            result[er_index][rank]['event_count'] += 1
        
        # デッキカラー単位のデータ格納
        json_data[deck_colors] = {
            'event_wins': result,
            'turn_info': get_won_by_turns_colors(deck_colors, won_by_turns_df)
        }

    # データをファイル出力
    with open(os.path.join(output_dir, 'wr_and_mana_curve.json'), 'w') as file:
        json.dump(json_data, file, separators=(',', ':'))


if __name__ =='__main__':
    main()
