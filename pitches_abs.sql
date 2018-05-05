DROP MATERIALIZED VIEW IF EXISTS PITCHES_ABS CASCADE;
CREATE MATERIALIZED VIEW PITCHES_ABS AS
SELECT p.*,
  a.away_team_runs,
  a.home_team_runs,
  a.b_height,
  a.balls,
  a.strikes,
  a.ab_start_outs,
  a.ab_end_outs,
  a.def_run_delta,
  a.ab_result,
  a.batter,
  a.pitcher,
  a.b_stands,
  a.p_throws,
  a.inning,
  a.inning_half,
  a.ab_des,
  a.event AS ab_event,
  a.ab_tfs,
  a.ab_tfs_z
FROM pitches_filter p JOIN ATBATS a ON ((p.game_id = a.game_id)
                                        AND (p.ab_num = a.ab_num))
ORDER BY p.game_id, p.ab_num, p.pitch_id;
