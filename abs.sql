DROP MATERIALIZED VIEW IF EXISTS ABS CASCADE;
CREATE MATERIALIZED VIEW ABS AS WITH rd AS
  ( SELECT CASE
               WHEN ((atbats.home_team_runs IS NULL)
                     OR (atbats.away_team_runs IS NULL)) THEN NULL::double precision
               WHEN (atbats.side = 'top'::text) THEN (atbats.home_team_runs - atbats.away_team_runs)
               ELSE (atbats.away_team_runs - atbats.home_team_runs)
           END AS run_delta,
           atbats.ab_num,
           atbats.side,
           atbats.game_id
   FROM atbats
   ORDER BY atbats.game_id,
            atbats.ab_num )
SELECT atbats.ab_num,
       atbats.away_team_runs,
       atbats.b AS balls,
       atbats.b_height,
       atbats.batter,
       atbats.des AS ab_des,
       atbats.event,
       atbats.game_id,
       atbats.home_team_runs,
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
                                                                                                                                   ORDER BY atbats.game_id,
                                                                                                                                            atbats.ab_num))
           ELSE lag(rd.run_delta, 1) OVER (
                                           ORDER BY rd.game_id,
                                                    rd.ab_num)
       END AS def_run_delta,
       atbats.inning,
       atbats.o AS ab_end_outs,
       CASE
           WHEN (lag(atbats.o, 1) OVER (
                                        ORDER BY atbats.game_id,
                                                 atbats.ab_num) IS NULL) THEN (0)::double precision
           ELSE lag(atbats.o, 1) OVER (
                                       ORDER BY atbats.game_id,
                                                atbats.ab_num)
       END AS ab_start_outs,
       atbats.p_throws,
       atbats.pitcher,
       atbats.s AS strikes,
       atbats.side AS inning_half,
       atbats.stand AS b_stands,
       atbats.start_tfs AS ab_tfs,
       atbats.start_tfs_zulu AS ab_tfs_z,
       CASE
           WHEN ((atbats.event = 'Single'::text)
                 OR (atbats.event = 'Double'::text)
                 OR (atbats.event = 'Triple'::text)
                 OR (atbats.event = 'Home Run'::text)
                 OR (atbats.event = 'Fan interference'::text)) THEN 'hit'::text
           WHEN ((atbats.event = 'Sac Bunt'::text)
                 OR (atbats.event = 'Sac Fly'::text)
                 OR (atbats.event = 'Walk'::text)
                 OR (atbats.event = 'Intent Walk'::text)
                 OR (atbats.event = 'Catcher Interference'::text)
                 OR (atbats.event = 'Runner Out'::text)
                 OR (atbats.event IS NULL)) THEN 'no at-bat'::text
           ELSE 'out'::text
       END AS ab_result
FROM (atbats
      JOIN rd ON (((atbats.game_id = rd.game_id)
                   AND (atbats.ab_num = rd.ab_num))))
ORDER BY atbats.game_id,
         atbats.ab_num;
