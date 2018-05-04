# -*- coding: utf-8 -*-

from numpy import nan as NaN
import pandas as pd
from db_utils import connect
from sqlalchemy.types import Integer, Float, Date, Text

con = connect('mlb_msmc')
stat_dtype_map = {'ab': Integer(), 'h': Integer(), 'bb': Integer(), 'hr': Integer(),
                  'rbi': Integer(), 'so': Integer(), 'r': Integer(), 'sb': Integer(),
                  'cs': Integer(), 'w': Integer(), 'l': Integer(), 'sv': Integer(),
                  'avg': Float(), 'ops': Float(), 'ip': Float(), 'whip': Float(),
                  'era': Float(), 'id': Text(), 'des': Text()}
dir_dtype_map = {'id': Text(), 'last_name':Text(), 'first_name': Text(), 'dob': Date(), 'type': Text(),
                 'height': Float(), 'weight': Float(), 'pos': Text(), 'current_position': Text(),
                 'bats': Text(), 'throws': Text()}

p_cols = ['player_id', 'game_id', 'career_ab', 'career_avg', 'career_bb', 'career_era', 'career_h', 'career_hr', 'career_ip', 'career_l', 'career_rbi', 'career_so', 'career_sv', 'career_w', 'career_whip', 'empty_ab', 'empty_avg', 'empty_bb', 'empty_era', 'empty_h', 'empty_hr', 'empty_ip', 'empty_rbi', 'empty_so', 'empty_whip', 'loaded_ab', 'loaded_avg', 'loaded_bb', 'loaded_era', 'loaded_h', 'loaded_hr', 'loaded_ip', 'loaded_rbi', 'loaded_so', 'loaded_whip', 'men_on_ab', 'men_on_avg', 'men_on_bb', 'men_on_era', 'men_on_h', 'men_on_hr', 'men_on_ip', 'men_on_rbi', 'men_on_so', 'men_on_whip', 'month_ab', 'month_avg', 'month_bb', 'month_des', 'month_era', 'month_h', 'month_hr', 'month_ip', 'month_rbi', 'month_so', 'month_whip', 'risp_ab', 'risp_avg', 'risp_bb', 'risp_era', 'risp_h', 'risp_hr', 'risp_ip', 'risp_rbi', 'risp_so', 'risp_whip', 'season_ab', 'season_avg', 'season_bb', 'season_era', 'season_h', 'season_hr', 'season_ip', 'season_l', 'season_rbi', 'season_so', 'season_sv', 'season_w', 'season_whip', 'team_ab', 'team_avg', 'team_bb', 'team_des', 'team_era', 'team_h', 'team_hr', 'team_ip', 'team_rbi', 'team_so', 'team_whip', 'vs_lhb_ab', 'vs_lhb_avg', 'vs_lhb_bb', 'vs_lhb_era', 'vs_lhb_h', 'vs_lhb_hr', 'vs_lhb_ip', 'vs_lhb_rbi', 'vs_lhb_so', 'vs_lhb_whip', 'vs_rhb_ab', 'vs_rhb_avg', 'vs_rhb_bb', 'vs_rhb_era', 'vs_rhb_h', 'vs_rhb_hr', 'vs_rhb_ip', 'vs_rhb_rbi', 'vs_rhb_so', 'vs_rhb_whip']

b_cols = ['player_id', 'game_id', 'career_ab', 'career_avg', 'career_bb', 'career_cs', 'career_h', 'career_hr', 'career_ops', 'career_r', 'career_rbi', 'career_sb', 'career_so', 'empty_ab', 'empty_avg', 'empty_bb', 'empty_cs', 'empty_h', 'empty_hr', 'empty_ops', 'empty_r', 'empty_rbi', 'empty_sb', 'empty_so', 'loaded_ab', 'loaded_avg', 'loaded_bb', 'loaded_cs', 'loaded_h', 'loaded_hr', 'loaded_ops', 'loaded_r', 'loaded_rbi', 'loaded_sb', 'loaded_so', 'men_on_ab', 'men_on_avg', 'men_on_bb', 'men_on_cs', 'men_on_h', 'men_on_hr', 'men_on_ops', 'men_on_r', 'men_on_rbi', 'men_on_sb', 'men_on_so', 'month_ab', 'month_avg', 'month_bb', 'month_cs', 'month_des', 'month_h', 'month_hr', 'month_ops', 'month_r', 'month_rbi', 'month_sb', 'month_so', 'risp_ab', 'risp_avg', 'risp_bb', 'risp_cs', 'risp_h', 'risp_hr', 'risp_ops', 'risp_r', 'risp_rbi', 'risp_sb', 'risp_so', 'season_ab', 'season_avg', 'season_bb', 'season_cs', 'season_h', 'season_hr', 'season_ops', 'season_r', 'season_rbi', 'season_sb', 'season_so', 'team_ab', 'team_avg', 'team_bb', 'team_cs', 'team_des', 'team_h', 'team_hr', 'team_ops', 'team_r', 'team_rbi', 'team_sb', 'team_so', 'vs_lhp_ab', 'vs_lhp_avg', 'vs_lhp_bb', 'vs_lhp_cs', 'vs_lhp_h', 'vs_lhp_hr', 'vs_lhp_ops', 'vs_lhp_r', 'vs_lhp_rbi', 'vs_lhp_sb', 'vs_lhp_so', 'vs_rhp_ab', 'vs_rhp_avg', 'vs_rhp_bb', 'vs_rhp_cs', 'vs_rhp_h', 'vs_rhp_hr', 'vs_rhp_ops', 'vs_rhp_r', 'vs_rhp_rbi', 'vs_rhp_sb', 'vs_rhp_so']

pitcher_dtypes = {stat: stat_dtype_map[stat.split('_')[-1]] for stat in p_cols}
batter_dtypes = {stat: stat_dtype_map[stat.split('_')[-1]] for stat in b_cols}

p_template = pd.DataFrame(columns=p_cols)
b_template = pd.DataFrame(columns=b_cols)

common_cats = ['career_', 'empty_', 'loaded_', 'men_on_', 'month_', 'risp_', 'season_', 'team_']
pitch_cats = ['vs_lhb_', 'vs_rhb_']
bat_cats = ['vs_lhp_', 'vs_rhp_']

def upload_stats(stat_dfs):
    pitchers = pd.concat([p_template] + stat_dfs['pitchers'], ignore_index=True)
    batters = pd.concat([b_template] + stat_dfs['batters'], ignore_index=True)
    pitchers.replace('-[\.-]*', NaN, regex=True, inplace=True)
    batters.replace('-[\.-]*', NaN, regex=True, inplace=True)
    pitchers = pitchers[p_cols]
    batters = batters[b_cols]
    for cat in common_cats + pitch_cats:
        pitchers[cat + 'avg'] = pitchers.apply(lambda r: r[cat + 'avg'] if (r[cat + 'ab'] != '0')
                                               else NaN, axis=1)
        pitchers[cat + 'era'] = pitchers.apply(lambda r: r[cat + 'era'] if (r[cat + 'ip'] != '0.0')
                                               else NaN, axis=1)
        pitchers[cat + 'whip'] = pitchers.apply(lambda r: r[cat + 'whip'] if (r[cat + 'ip'] != '0.0')
                                                else NaN, axis=1)
    for cat in common_cats + bat_cats:
        batters[cat + 'avg'] = batters.apply(lambda r: r[cat + 'avg'] if (r[cat + 'ab'] != '0')
                                             else NaN, axis=1)
        batters[cat + 'ops'] = batters.apply(lambda r: r[cat + 'ops'] if ((r[cat + 'ab'] != '0') or
                                                                          (r[cat + 'bb'] != '0'))
                                             else NaN, axis=1)
    pitchers.to_sql('pitcher_stats', con, if_exists='append', index=False,
                    dtype=pitcher_dtypes)
    batters.to_sql('batter_stats', con, if_exists='append', index=False,
                    dtype=batter_dtypes)
    return


def upload_directory(dir_dicts):
    player_dir = pd.DataFrame(dir_dicts)
    player_dir = player_dir[['id', 'last_name', 'first_name', 'dob', 'type', 'height',
                             'weight', 'pos', 'current_position', 'bats', 'throws']]
    player_dir.sort_values(by='id', inplace=True)
    dir_dtypes = {item: dir_dtype_map[item] for item in player_dir.columns}
    player_dir.to_sql('players', con, if_exists='append', index=False,
                      dtype=dir_dtypes)
    return


def close_con():
    con.close()
    return
