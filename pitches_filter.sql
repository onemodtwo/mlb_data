DROP VIEW IF EXISTS PITCHES_FILTER CASCADE;
CREATE VIEW PITCHES_FILTER AS
WITH p_no_x AS
(
  SELECT p.ab_num,
    p.des AS pitch_des,
    p.end_speed,
    p.game_id,
    p.id AS pitch_id,
    p.on_1b,
    p.on_2b,
    p.on_3b,
    p.px,
    p.py,
    p.pz,
    p.pfx_x,
    p.pfx_z,
    p.sz_top,
    p.sz_bot,
    ((0.5) * (p.sz_top - p.sz_bot)) AS centered_sz_top,
    ((0.5) * (p.sz_bot - p.sz_top)) AS centered_sz_bot,
    p.start_speed,
    p.sv_id,
    p.tfs AS pitch_tfs,
    p.tfs_zulu AS pitch_tfs_z,
    CASE
      WHEN (p.type = 'S') THEN 'S'
      WHEN (p.type = 'B') THEN 'B'
      WHEN ((p.type = 'X') AND (p.des ~~ '%out(s)%')) THEN 'X'
      ELSE 'H'
    END AS pitch_result
  FROM pitches_raw p JOIN games_raw g ON (p.game_id = g.game_id)
  WHERE ((g.game_type <> 'S') AND (g.game_type <> 'A'))
), avg_szs AS
(
  SELECT a.batter,
    avg(p.sz_top) AS avg_sz_top,
    avg(p.sz_bot) AS avg_sz_bot
  FROM pitches_raw p JOIN atbats_raw a ON ((p.game_id = a.game_id)
                                           AND (p.ab_num = a.ab_num))
  GROUP BY a.batter
), ab_avgs AS
(
  SELECT ab.ab_num,
    ab.game_id,
    av.avg_sz_top,
    av.avg_sz_bot
  FROM atbats_raw ab JOIN avg_szs av ON ab.batter=av.batter
)
SELECT p_no_x.*,
  aba.avg_sz_top,
  aba.avg_sz_bot,
  p_no_x.pz - ((0.5) * (aba.avg_sz_top - aba.avg_sz_bot)) AS adj_py
FROM p_no_x LEFT JOIN ab_avgs aba ON ((p_no_x.game_id=aba.game_id)
                                      AND (p_no_x.ab_num=aba.ab_num));
