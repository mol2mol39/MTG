import pandas as pd
import uuid
import csv
import time

from postgresql.interface import PostgresInterface


# 対象のカードセット
EXPANSION = "WOE"

PREFIX = ["opening_hand_", "drawn_", "tutored_", "deck_", "sideboard_"]
OPENING_HAND = 0
DRAWN = 1
TUTORED = 2
DECK = 3
SIDEBOARD = 4

# カード名のカラム取得
df = pd.read_csv(
    f'data/game_data_public.{EXPANSION}.PremierDraft.csv',
    nrows=0
)

OPENING_HAND_COLUMNS = [
    x for x in df.columns if x.startswith(PREFIX[OPENING_HAND])
]
DRAWN_COLUMNS = [x for x in df.columns if x.startswith(PREFIX[DRAWN])]
TUTORED_COLUMNS = [x for x in df.columns if x.startswith(PREFIX[TUTORED])]
DECK_COLUMNS = [x for x in df.columns if x.startswith(PREFIX[DECK])]
SIDEBOARD_COLUMNS = [x for x in df.columns if x.startswith(PREFIX[SIDEBOARD])]

if not (len(OPENING_HAND_COLUMNS) == len(DRAWN_COLUMNS) == len(TUTORED_COLUMNS)
        == len(DECK_COLUMNS) == len(SIDEBOARD_COLUMNS)):
    print("CSVのカラム定義に誤りがあります。")

columns_dict = {
    OPENING_HAND: OPENING_HAND_COLUMNS,
    DRAWN: DRAWN_COLUMNS,
    TUTORED: TUTORED_COLUMNS,
    DECK: DECK_COLUMNS,
    SIDEBOARD: SIDEBOARD_COLUMNS
}


# Cardsテーブルへのバルクインサートするためのクラス
class Cards:
    cards_list = None  # CARDSテーブルに格納するデータ

    def __init__(self, db: PostgresInterface):
        self.cards_list = []
        self.db = db

    # CSVのカラム名からプレフィックスを取り除いてカード名を返す
    def _remove_prefix(self, column_name):
        for prefix in PREFIX:
            column_name = column_name.replace(prefix, "")
        return column_name

    # CARDSテーブルに登録用のデータを格納する
    def _register_cards(self, row, cards_type, cards_id):
        for col in columns_dict[cards_type]:
            if row[col] > 0:
                self.cards_list.append(
                    (cards_id, cards_type, self._remove_prefix(col), row[col])
                )

    # applyで適用される関数。
    def create_uuid_and_cards_data(self, row, cards_type):
        cards_id = uuid.uuid4()

        self._register_cards(row, cards_type, cards_id)
        return cards_id


# チャンク単位の処理
# USER, DECK, GAME, CARDSテーブルにインサート
# 重複データがあるため、INSERTでエラーにならないように考慮する
def insert_df(df, db):
    cards = Cards(db)
    user_col_df = df[db.COLUMNS['USER']].copy()
    df_user = user_col_df.groupby('draft_id', as_index=False).agg('first')
    df_user['rank'] = df_user['rank'].fillna("")
    db.insert_user(df_user.values)

    deck_columns = db.COLUMNS['DECK'][:-2] + columns_dict[DECK] \
        + columns_dict[SIDEBOARD]
    deck_col_df = df[deck_columns].copy()
    df_deck = deck_col_df.groupby(
        ['draft_id', 'build_index'], as_index=False
    ).agg('first')
    df_deck['splash_colors'] = df_deck['splash_colors'].fillna("")
    df_deck.loc[:, 'deck_id'] = df_deck.apply(
        cards.create_uuid_and_cards_data,
        axis=1,
        args=(DECK,)
    )
    df_deck.loc[:, 'sideboard_id'] = df_deck.apply(
        cards.create_uuid_and_cards_data,
        axis=1, args=(SIDEBOARD,)
    )
    db.insert_deck(df_deck[db.COLUMNS['DECK']].values)

    game_columns = db.COLUMNS['GAME'][:-3] + columns_dict[OPENING_HAND] \
        + columns_dict[DRAWN] + columns_dict[TUTORED]
    df_game = df[game_columns].copy()
    df_game.drop_duplicates(subset=['draft_id', 'match_number'], inplace=True)
    df_game['opp_colors'] = df_game['opp_colors'].fillna("")
    df_game.loc[:, 'opening_hand_id'] = df_game.apply(
        cards.create_uuid_and_cards_data,
        axis=1,
        args=(OPENING_HAND,)
    )
    df_game.loc[:, 'drawn_id'] = df_game.apply(
        cards.create_uuid_and_cards_data,
        axis=1,
        args=(DRAWN,)
    )
    df_game.loc[:, 'tutored_id'] = df_game.apply(
        cards.create_uuid_and_cards_data,
        axis=1,
        args=(TUTORED,)
    )
    db.insert_game(df_game[db.COLUMNS['GAME']].values)

    # cards.create_uuid_and_cards_data関数で処理したデータをまとめてインサートする
    db.insert_cards(cards.cards_list)


def main():
    db = PostgresInterface(EXPANSION)
    chunk_size = 2000
    df_chunk = pd.read_csv(
        f'data/game_data_public.{EXPANSION}.PremierDraft.csv',
        chunksize=chunk_size,
        parse_dates=['draft_time', 'game_time']
    )

    all_start_time = time.time()

    for i, chunk in enumerate(df_chunk):
        start_time = time.time()
        insert_df(chunk, db)
        end_time = time.time()
        print(f"{chunk_size * (i + 1)}行の登録完了."
              f"処理時間: {(end_time - start_time) / 60}分")

    all_end_time = time.time()
    print(f"処理完了。処理時間: {(all_end_time - all_start_time) / 60}分")


if __name__ == "__main__":
    main()
