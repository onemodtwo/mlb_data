DROP MATERIALIZED VIEW IF EXISTS ATBATS CASCADE;
CREATE MATERIALIZED VIEW ATBATS AS WITH rd AS
  ( SELECT CASE
               WHEN ((atbats_raw.home_team_runs IS NULL)
                     OR (atbats_raw.away_team_runs IS NULL)) THEN NULL::double precision
               WHEN (atbats_raw.side = 'top'::text) THEN (atbats_raw.home_team_runs - atbats_raw.away_team_runs)
               ELSE (atbats_raw.away_team_runs - atbats_raw.home_team_runs)
           END AS run_delta,
           atbats_raw.ab_num,
           atbats_raw.side,
           atbats_raw.game_id
   FROM atbats_raw
   ORDER BY atbats_raw.game_id,
            atbats_raw.ab_num )
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
           WHEN ((rd.ab_num < lag(rd.ab_num, 1) OVER (
                                                      ORDER BY rd.game_id,
                                                               rd.ab_num))
                 OR ((rd.side <> lag(rd.side, 1) OVER (
                                                       ORDER BY rd.game_id,
                                                                rd.ab_num))
                     AND (lag(rd.run_delta, 1) OVER (
                                                     ORDER BY rd.game_id,
                                                              rd.ab_num) = (0)::double precision))) THEN (0)::double precision
           WHEN ((rd.side <> lag(rd.side, 1) OVER (
                                                   ORDER BY rd.game_id,
                                                            rd.ab_num))
                 AND (lag(rd.run_delta, 1) OVER (
                                                 ORDER BY rd.game_id,
                                                          rd.ab_num) <> (0)::double precision)) THEN (- lag(rd.run_delta, 1) OVER (
                                                                                                                                   ORDER BY atbats_raw.game_id,
                                                                                                                                            atbats_raw.ab_num))
           ELSE lag(rd.run_delta, 1) OVER (
                                           ORDER BY rd.game_id,
                                                    rd.ab_num)
       END AS def_run_delta,
       atbats_raw.inning,
       atbats_raw.o AS ab_end_outs,
       CASE
           WHEN (lag(atbats_raw.o, 1) OVER (
                                        ORDER BY atbats_raw.game_id,
                                                 atbats_raw.ab_num) IS NULL) THEN (0)::double precision
           ELSE lag(atbats_raw.o, 1) OVER (
                                       ORDER BY atbats_raw.game_id,
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
           WHEN ((atbats_raw.event = 'Single'::text)
                 OR (atbats_raw.event = 'Double'::text)
                 OR (atbats_raw.event = 'Triple'::text)
                 OR (atbats_raw.event = 'Home Run'::text)
                 OR (atbats_raw.event = 'Fan interference'::text)) THEN 'hit'::text
           WHEN ((atbats_raw.event = 'Sac Bunt'::text)
                 OR (atbats_raw.event = 'Sac Fly'::text)
                 OR (atbats_raw.event = 'Walk'::text)
                 OR (atbats_raw.event = 'Intent Walk'::text)
                 OR (atbats_raw.event = 'Catcher Interference'::text)
                 OR (atbats_raw.event = 'Runner Out'::text)
                 OR (atbats_raw.event IS NULL)) THEN 'no at-bat'::text
           ELSE 'out'::text
       END AS ab_result
FROM (atbats_raw
      JOIN rd ON (((atbats_raw.game_id = rd.game_id)
                   AND (atbats_raw.ab_num = rd.ab_num))))
ORDER BY atbats_raw.game_id,
         atbats_raw.ab_num;
