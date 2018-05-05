DROP MATERIALIZED VIEW IF EXISTS BATTING_AVGS CASCADE;
CREATE MATERIALIZED VIEW BATTING_AVGS AS
WITH hits AS
(
  SELECT ATBATS.batter,count(*) AS num_hits
  FROM ATBATS
  WHERE ATBATS.ab_result = 'hit'
  GROUP BY ATBATS.batter
), outs AS
(
  SELECT ATBATS.batter,count(*) AS num_outs
  FROM ATBATS
  WHERE ATBATS.ab_result = 'out'
  GROUP BY ATBATS.batter
)
SELECT h.batter,
  h.num_hits / (h.num_hits + o.num_outs) AS batting_avg,
  h.num_hits + o.num_outs AS plate_appearances
FROM hits h JOIN outs o ON h.batter = o.batter
ORDER BY (h.num_hits / (h.num_hits + o.num_outs)) DESC;
