DROP VIEW IF EXISTS PITCHES_FILTER CASCADE;
CREATE VIEW PITCHES_FILTER AS WITH p_no_x AS
  ( SELECT p.ab_num,
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
           ((0.5)::double precision * (p.sz_top - p.sz_bot)) AS centered_sz_top,
           ((0.5)::double precision * (p.sz_bot - p.sz_top)) AS centered_sz_bot,
           p.start_speed,
           p.sv_id,
           p.tfs AS pitch_tfs,
           p.tfs_zulu AS pitch_tfs_z,
           CASE
               WHEN (p.type = 'S'::text) THEN 'S'::text
               WHEN (p.type = 'B'::text) THEN 'B'::text
               WHEN ((p.type = 'X'::text)
                     AND (p.des ~~ '%out(s)%'::text)) THEN 'X'::text
               ELSE 'H'::text
           END AS pitch_result
   FROM (pitches p
         JOIN games ON ((p.game_id = games.game_id)))
   WHERE ((games.game_type <> 'S'::text)
          AND (games.game_type <> 'A'::text)) ),
            avg_szs AS
  (SELECT a.batter,
          avg(p.sz_top) AS avg_sz_top,
          avg(p.sz_bot) AS avg_sz_bot
   FROM (pitches p
         JOIN atbats a ON (((p.game_id = a.game_id)
                            AND (p.ab_num = a.ab_num))))
   GROUP BY a.batter),
            ab_avgs AS
  ( SELECT ab.ab_num,
           ab.game_id,
           av.avg_sz_top,
           av.avg_sz_bot
   FROM atbats ab
   JOIN avg_szs av ON ab.batter=av.batter)
SELECT p_no_x.*,
       aba.avg_sz_top,
       aba.avg_sz_bot,
       p_no_x.pz - ((0.5)::double precision * (aba.avg_sz_top - aba.avg_sz_bot)) AS adj_py
FROM p_no_x
LEFT JOIN ab_avgs aba ON p_no_x.game_id=aba.game_id
AND p_no_x.ab_num=aba.ab_num ;
