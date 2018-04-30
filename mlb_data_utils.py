# -*- coding: utf-8 -*-

from numpy import nan as NaN
import pandas as pd
from datetime import datetime
from db_utils import connect
from sqlalchemy.types import Integer, Float, Boolean, DateTime, Text

conn = connect('mlb_msmc')


def get_game_dict(game_info, game_id):
    game_dict = {'game_id': game_id}
    if game_info:
        if 'type' in game_info.game.attrs:
            game_dict['game_type'] = game_info.game['type']
        else:
            game_dict['game_type'] = None
        if game_dict['game_type'] == 'S':
            game_dict['game_type_des'] = 'Spring Training'
        elif game_dict['game_type'] == 'R':
            game_dict['game_type_des'] = 'Regular Season'
        elif game_dict['game_type'] == 'F':
            game_dict['game_type_des'] = 'Wild-card Game'
        elif game_dict['game_type'] == 'D':
            game_dict['game_type_des'] = 'Divisional Series'
        elif game_dict['game_type'] == 'L':
            game_dict['game_type_des'] = 'LCS'
        elif game_dict['game_type'] == 'W':
            game_dict['game_type_des'] = 'World Series'
        elif game_dict['game_type'] == 'A':
            game_dict['game_type_des'] = 'All-Star Game'
        else:
            game_dict['game_type_des'] = None
        if 'local_game_time' in game_info.game.attrs:
            date = [int(x) for x in game_id.split('_')[:3]]
            time = [int(x) for x in
                    game_info.game['local_game_time'].split(':')]
            game_dict['local_game_time'] = datetime(*tuple(date + time))
        else:
            game_dict['local_game_time'] = None
        if 'game_pk' in game_info.game.attrs:
            game_dict['game_pk'] = game_info.game['game_pk']
        else:
            game_dict['game_pk'] = None
        if game_info.find('team'):
            game_dict['home_team_id'] = \
                game_info.find('team', type='home')['code']
            game_dict['away_team_id'] = \
                game_info.find('team', type='away')['code']
            game_dict['home_team_lg'] = \
                game_info.find('team', type='home')['league']
            game_dict['away_team_lg'] = \
                game_info.find('team', type='away')['league']
        else:
            game_dict['home_team_id'] = None
            game_dict['away_team_id'] = None
            game_dict['home_team_lg'] = None
            game_dict['away_team_lg'] = None
        if game_dict['home_team_lg'] == game_dict['away_team_lg']:
            game_dict['interleague'] = False
        else:
            game_dict['interleague'] = True
        if game_info.find('stadium'):
            game_dict['park_id'] = game_info.stadium['id']
            game_dict['park_name'] = game_info.stadium['name']
            game_dict['park_loc'] = game_info.stadium['location']
        else:
            game_dict['park_id'] = 'None'
            game_dict['park_name'] = 'None'
            game_dict['park_loc'] = None
    else:
        game_dict['game_type'] = None
        game_dict['game_type_des'] = None
        game_dict['local_game_time'] = None
        game_dict['game_pk'] = None
        game_dict['home_team_id'] = None
        game_dict['away_team_id'] = None
        game_dict['home_team_lg'] = None
        game_dict['away_team_lg'] = None
        game_dict['interleague'] = None
        game_dict['park_id'] = None
        game_dict['park_name'] = None
        game_dict['park_loc'] = None
    return game_dict


def get_ab_pitches(innings, game_id):
    ab_dfs, p_dfs = [], []
    for inning in innings:
        if inning.find('bottom'):
            sides = [inning.top, inning.bottom]
        elif inning.find('top'):
            sides = [inning.top]
        else:
            sides = []
        for side in sides:
            try:
                atbats = side.find_all('atbat')
            except:
                continue
            ab = pd.DataFrame([atbat.attrs for atbat in atbats])
            ab['game_id'] = game_id
            ab['inning'] = inning.attrs['num']
            ab['side'] = side.name
            ab_dfs.append(ab)
            for atbat in atbats:
                try:
                    pitches = atbat.find_all('pitch')
                except:
                    continue
                p = pd.DataFrame([pitch.attrs for pitch in pitches])
                p['game_id'] = game_id
                p['ab_num'] = atbat['num']
                p_dfs.append(p)
    return ab_dfs, p_dfs


def fix_local_time(row, tfs=None):
    if row[tfs]:
        d = [int(x) for x in row['game_id'].split('_')[:3]]
        t = [int(row[tfs][i: i + 2]) for i in [0, 2, 4]]
        return datetime(*tuple(d + t))
    else:
        return None


def fix_df(dfs, kind='ab'):
    ab_vars = ['away_team_runs', 'home_team_runs', 'b', 's', 'o',
               'inning', 'b_height']
    p_vars = ['ax', 'ay', 'az', 'vx0', 'vy0', 'vz0', 'x', 'y', 'x0', 'y0',
              'z0', 'px', 'pz', 'pfx_x', 'pfx_z', 'break_angle', 'break_length',
              'break_y', 'start_speed', 'end_speed', 'spin_dir', 'spin_rate',
              'sz_bot', 'sz_top', 'type_confidence', 'nasty', 'zone']
    df = pd.concat(dfs, ignore_index=True)
    if kind == 'ab':
        vars = [var for var in ab_vars if var in df.columns]
        tfs = 'start_tfs'
        if 'b_height' in df.columns:
            df['b_height'] = df['b_height'].apply(lambda b_h:
                                                  12 * int(b_h.split('-')[0]) +
                                                  int(b_h.split('-')[1]))
        df['ab_num'] = df['num']
        df.drop('num', axis=1, inplace=True)
    else:
        vars = [var for var in p_vars if var in df.columns]
        tfs = 'tfs'
        if 'sv_id' in df.columns:
            df['sv_id'] = pd.to_datetime(df['sv_id'], errors='coerce',
                                         format='%y%m%d_%H%M%S')
    df['ab_num'] = df['ab_num'].astype('int32', errors='ignore')
    for col in vars:
        df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
    if (tfs + '_zulu') in df.columns:
        df[tfs + '_zulu'] = pd.to_datetime(df[tfs + '_zulu'], errors='coerce',
                                           format='%Y-%m-%dT%H:%M:%SZ')
    if tfs in df.columns:
        df[tfs] = df.apply(fix_local_time, axis=1, tfs=tfs)
    if 'pz' in df.columns and 'sz_top ' in df.columns and 'sz_bot' in df.columns:
        df['py'] = df['pz'] - 0.5 * (df['sz_bot'] + df['sz_top'])
    return df


def dump_records(games, atbats, pitches):
    type_dict = {'int32': Integer(), 'int64': Integer(), 'float32': Float(),
                 'float64': Float(), 'object': Text(), 'bool': Boolean(),
                 'datetime64[ns]': DateTime()}
    game_dtypes = {v: type_dict[str(games.dtypes[v])] for v in games.columns}
    atbat_dtypes = {v: type_dict[str(atbats.dtypes[v])]
                    for v in atbats.columns}
    pitch_dtypes = {v: type_dict[str(pitches.dtypes[v])]
                    for v in pitches.columns}
    games.to_sql('games', conn, if_exists='append', index=False,
                 dtype=game_dtypes)
    atbats.to_sql('atbats_raw', conn, if_exists='append', index=False,
                  dtype=atbat_dtypes)
    pitches.to_sql('pitches_raw', conn, if_exists='append', index=False,
                   dtype=pitch_dtypes)
    return


def finish_up(ab_dfs, p_dfs, game_dicts, retro_fixes):
    pitch_cols = ['ab_num', 'ax', 'ay', 'az', 'break_angle', 'break_length',
                  'break_y', 'des', 'end_speed', 'game_id', 'id', 'nasty',
                  'on_1b', 'on_2b', 'on_3b', 'pfx_x', 'pfx_z', 'pitch_type',
                  'px', 'py', 'pz', 'spin_dir', 'spin_rate', 'start_speed',
                  'sv_id', 'sz_bot', 'sz_top', 'tfs', 'tfs_zulu', 'type',
                  'type_confidence', 'vx0', 'vy0', 'vz0', 'x', 'y', 'x0',
                  'y0', 'z0', 'zone']
    atbat_cols = ['ab_num', 'away_team_runs', 'b', 'b_height', 'batter', 'des',
                  'event', 'event_num', 'game_id', 'home_team_runs', 'inning',
                  'o', 'p_throws', 'pitcher', 's', 'side', 'stand',
                  'start_tfs', 'start_tfs_zulu']
    games = pd.DataFrame(game_dicts)
    ixs = games[games['game_id'].isin(retro_fixes)].index
    games.loc[ixs, 'retro_game_id'] = games.loc[ixs, 'retro_game_id'].apply(
        lambda r_id: r_id[:-1] + '1')
    atbats = fix_df(ab_dfs)
    pitches = fix_df(p_dfs, kind='pitch')
    missing_ab_cols = [col for col in atbat_cols if col not in atbats.columns]
    missing_p_cols = [col for col in pitch_cols if col not in pitches.columns]
    for col in missing_ab_cols:
        atbats[col] = NaN
    for col in missing_p_cols:
        pitches[col] = NaN
    dump_records(games, atbats[atbat_cols], pitches[pitch_cols])
    return


def close_con():
    conn.close()
    return
