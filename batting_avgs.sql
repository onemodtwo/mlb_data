DROP MATERIALIZED VIEW IF EXISTS BATTING_AVGS CASCADE;
CREATE MATERIALIZED VIEW BATTING_AVGS AS WITH hits AS
  (SELECT abs.batter,
          count(*) AS num_hits
   FROM ABS
   WHERE abs.ab_result = 'hit'::text
   GROUP BY abs.batter),
                         outs AS
  (SELECT abs.batter,
          count(*) AS num_outs
   FROM ABS
   WHERE abs.ab_result = 'out'::text
   GROUP BY abs.batter)
SELECT h.batter,
       h.num_hits::double precision / (h.num_hits + o.num_outs)::double precision AS batting_avg,
       h.num_hits + o.num_outs AS plate_appearances
FROM hits h
JOIN outs o ON h.batter = o.batter
ORDER BY (h.num_hits::double precision / (h.num_hits + o.num_outs)::double precision) DESC;
