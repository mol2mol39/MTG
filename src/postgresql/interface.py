import psycopg2
from psycopg2 import extras
import pandas as pd


class PostgresInterface:
    # DB接続情報
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'MTG'
    DB_USER = 'postgres'
    DB_PASS = 'password'

    COLUMNS = {
        'USER': ['draft_id', 'draft_time', 'rank', 'user_n_games_bucket', 'user_game_win_rate_bucket'],
        'DECK': ['draft_id', 'build_index', 'main_colors', 'splash_colors', 'deck_id', 'sideboard_id'],
        'GAME': ['draft_id', 'game_time', 'build_index', 'match_number', 'opp_rank', 'on_play', 'num_mulligans',
                 'opp_num_mulligans', 'opp_colors', 'num_turns', 'won', 'opening_hand_id', 'drawn_id', 'tutored_id']
    }
    # PythonのUUID型をINSERTできるようにする
    psycopg2.extras.register_uuid()

    def __init__(self, expansion):
        self.expansion = expansion

    def _get_connection(self):
        return psycopg2.connect(
            f"postgresql://{self.DB_USER}:{self.DB_PASS}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # バルクインサート実行
    def _exe_values(self, query, params):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, params)

    # select文実行(データフレームを返す)
    def _exe_select_df(self, query, params=[]):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=columns)
        return df
    
    # select文実行（辞書型で返す）
    def _exe_select_dict(self, query, params=[]):
        results = []
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                results = [dict(row) for row in rows]
        return results

    def insert_user(self, values):
        sql = f"""
              INSERT INTO {self.expansion}.USER ({','.join(self.COLUMNS["USER"])})
                SELECT {','.join([f"v.{col}" for col in self.COLUMNS['USER']])}
                FROM (VALUES %s) AS v({','.join(self.COLUMNS["USER"])})
                WHERE NOT EXISTS (
                  SELECT 1 FROM {self.expansion}.USER WHERE draft_id = v.draft_id
                );
              """
        try:
            self._exe_values(sql, values)
        except Exception as e:
            self.output_error_data('user', values, e)

    def insert_deck(self, values):
        sql = f"""
              INSERT INTO {self.expansion}.DECK ({','.join(self.COLUMNS["DECK"])})
                SELECT {','.join([f"v.{col}" for col in self.COLUMNS['DECK']])}
                FROM (VALUES %s) AS v({','.join(self.COLUMNS["DECK"])})
                WHERE NOT EXISTS (
                  SELECT 1 FROM {self.expansion}.DECK
                  WHERE draft_id = v.draft_id AND build_index = v.build_index
                );
              """
        try:
            self._exe_values(sql, values)
        except Exception as e:
            self.output_error_data('deck', values, e)

    def insert_game(self, values):
        sql = f"""
              INSERT INTO {self.expansion}.GAME ({','.join(self.COLUMNS['GAME'])})
                SELECT {','.join([f"v.{col}" for col in self.COLUMNS['GAME']])}
                FROM (VALUES %s) AS v({','.join(self.COLUMNS['GAME'])})
                WHERE NOT EXISTS (
                  SELECT 1 FROM {self.expansion}.GAME
                  WHERE draft_id = v.draft_id AND match_number = v.match_number
                );
              """
        try:
            self._exe_values(sql, values)
        except Exception as e:
            self.output_error_data('game', values, e)

    def insert_cards(self, values):
        sql = f"INSERT INTO {self.expansion}.CARDS VALUES %s;"
        try:
            self._exe_values(sql, values)
        except Exception as e:
            self.output_error_data('cards', values, e)

    def insert_card_master(self, values):
        sql = "INSERT INTO CARD_MASTER VALUES %s;"
        self._exe_values(sql, values)

    def output_error_data(self, mode, values, err):
        print(f"{mode}のINSERT処理でエラー. {err}")
        with open(f"data/error_data_{mode}.txt", "a") as file:
            for val in values:
                file.write(str(val) + '\n')

    def select_game_data(self, return_value_type):
        sql = f"""
              SELECT d.main_colors, g.opp_colors, g.num_turns, g.won, count(*)
                FROM {self.expansion}.DECK d
                JOIN {self.expansion}.GAME g
                  ON g.draft_id = d.draft_id AND g.build_index = d.build_index
                WHERE LENGTH(d.main_colors) = 2 AND LENGTH(g.opp_colors) = 2
                GROUP BY d.main_colors, g.won, g.opp_colors, g.num_turns;
              """
        if return_value_type == 'df':
            return self._exe_select_df(sql)
        else:
            return self._exe_select_dict(sql)
