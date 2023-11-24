import pandas as pd
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from postgresql.interface import PostgresInterface


def main():
    db = PostgresInterface("public")

    # セットごとに基本土地を登録せず、基本土地は統一する
    basic_lands = ['Plains', 'Island', 'Swamp', 'Mountain', 'Forest']
    land_colors = ['W', 'U', 'B', 'R', 'G']
    basic_lands_values = [
        (
            10000 + i,
            "PUBLIC",
            basic_lands[i],
            "basic", land_colors[i],
            0,
            f"Basic Land - {basic_lands[i]}", True
        )
        for i in range(5)
    ]
    db.insert_card_master(basic_lands_values)

    # 基本土地以外のレコードを登録
    df_master = pd.read_csv('data/cards.csv')
    df_master = df_master[~df_master['name'].isin(basic_lands)]
    db.insert_card_master(df_master.values)


if __name__ == '__main__':
    main()
