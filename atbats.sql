DROP MATERIALIZED VIEW IF EXISTS ATBATS CASCADE;
CREATE MATERIALIZED VIEW ATBATS AS
WITH rd AS
(
  SELECT
    CASE
      WHEN ((atbats_raw.home_team_runs IS NULL)
             OR (atbats_raw.away_team_runs IS NULL))
        THEN NULL
      WHEN (atbats_raw.side = 'top')
        THEN (atbats_raw.home_team_runs - atbats_raw.away_team_runs)
      ELSE (atbats_raw.away_team_runs - atbats_raw.home_team_runs)
    END AS run_delta,
    atbats_raw.ab_num,
    atbats_raw.game_id
  FROM atbats_raw
  ORDER BY atbats_raw.game_id, atbats_raw.ab_num
)
SELECT atbats_raw.ab_num,
  atbats_raw.away_team_runs,
  atbats_raw.b AS balls,
  atbats_raw.b_height,
  atbats_raw.batter,
  atbats_raw.des AS ab_des,
  atbats_raw.event,
  atbats_raw.game_id,
  atbats_raw.home_team_runs,
  CASE
    WHEN ((rd.ab_num < lag(rd.ab_num, 1) OVER (ORDER BY rd.game_id, rd.ab_num))
          OR ((rd.side <> lag(rd.side, 1) OVER (ORDER BY rd.game_id, rd.ab_num))
              AND (lag(rd.run_delta, 1) OVER (ORDER BY rd.game_id, rd.ab_num)
                   = 0)))
      THEN (0)
    WHEN ((rd.side <> lag(rd.side, 1) OVER (ORDER BY rd.game_id, rd.ab_num))
          AND (lag(rd.run_delta, 1) OVER (ORDER BY rd.game_id, rd.ab_num) <> 0))
      THEN (- lag(rd.run_delta, 1) OVER (ORDER BY atbats_raw.game_id,
                                                  atbats_raw.ab_num))
    ELSE lag(rd.run_delta, 1) OVER (ORDER BY rd.game_id, rd.ab_num)
  END AS def_run_delta,
  atbats_raw.inning,
  atbats_raw.o AS ab_end_outs,
  CASE
    WHEN ((atbats_raw.ab_num < lag(atbats_raw.ab_num, 1) OVER
           (ORDER BY atbats_raw.game_id, atbats_raw.ab_num))
          OR (atbats_raw.side <> lag(atbats_raw.side, 1) OVER
              (ORDER BY rd.game_id, rd.ab_num)))
      THEN (0)
    ELSE lag(atbats_raw.o, 1) OVER (ORDER BY atbats_raw.game_id,
                                             atbats_raw.ab_num)
  END AS ab_start_outs,
  atbats_raw.p_throws,
  atbats_raw.pitcher,
  atbats_raw.s AS strikes,
  atbats_raw.side AS inning_half,
  atbats_raw.stand AS b_stands,
  atbats_raw.start_tfs AS ab_tfs,
  atbats_raw.start_tfs_zulu AS ab_tfs_z,
  CASE
    WHEN ((atbats_raw.event = 'Single')
          OR (atbats_raw.event = 'Double')
          OR (atbats_raw.event = 'Triple')
          OR (atbats_raw.event = 'Home Run')
          OR (atbats_raw.event = 'Fan interference')) THEN 'hit'
    WHEN ((atbats_raw.event = 'Sac Bunt')
          OR (atbats_raw.event = 'Sac Fly')
          OR (atbats_raw.event = 'Walk')
          OR (atbats_raw.event = 'Intent Walk')
          OR (atbats_raw.event = 'Catcher Interference')
          OR (atbats_raw.event = 'Runner Out')
          OR (atbats_raw.event IS NULL)) THEN 'no at-bat'
    ELSE 'out'
  END AS ab_result
FROM (atbats_raw JOIN rd ON (((atbats_raw.game_id = rd.game_id)
                             AND (atbats_raw.ab_num = rd.ab_num))))
ORDER BY atbats_raw.game_id, atbats_raw.ab_num;
