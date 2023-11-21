\c MTG

CREATE TABLE CARD_MASTER (
  id INTEGER PRIMARY KEY,
  expansion VARCHAR(10),
  card_name VARCHAR(100),
  rarity VARCHAR(8),
  color_identity VARCHAR(5),
  mana_value INTEGER,
  types VARCHAR(150),
  is_booster BOOLEAN
);

CREATE SCHEMA WOE;

CREATE TABLE WOE.USER (
  draft_id VARCHAR(50) PRIMARY KEY,
  draft_time TIMESTAMP,
  rank VARCHAR(8),
  user_n_games_bucket INTEGER,
  user_game_win_rate_bucket NUMERIC
);

CREATE TABLE WOE.DECK (
  draft_id VARCHAR(50),
  build_index INTEGER,
  main_colors VARCHAR(5),
  splash_colors VARCHAR(5),
  deck_id UUID,
  sideboard_id UUID,
  PRIMARY KEY (draft_id, build_index),
  FOREIGN KEY (draft_id) REFERENCES WOE.USER(draft_id)
);

CREATE TABLE WOE.GAME (
  draft_id VARCHAR(50),
  game_time TIMESTAMP,
  build_index INTEGER,
  match_number INTEGER,
  opp_rank VARCHAR(8),
  on_play BOOLEAN,
  num_mulligans INTEGER,
  opp_num_mulligans INTEGER,
  opp_colors VARCHAR(5),
  num_turns INTEGER,
  won BOOLEAN,
  opening_hand_id UUID,
  drawn_id UUID,
  tutored_id UUID,
  PRIMARY KEY (draft_id, match_number),
  FOREIGN KEY (draft_id) REFERENCES WOE.USER(draft_id)
);

CREATE TABLE WOE.CARDS (
  id UUID,
  cards_type INTEGER,
  card_name VARCHAR(100),
  num INTEGER
  -- FOREIGN KEY (card_id) REFERENCES CARD_MASTER(id)
);