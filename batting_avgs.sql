DROP MATERIALIZED VIEW IF EXISTS BATTING_AVGS CASCADE;
CREATE MATERIALIZED VIEW BATTING_AVGS AS
WITH hits AS
(
  SELECT ATBATS.batter, count(*) AS num_hits
  FROM ATBATS
  WHERE ATBATS.ab_result = 'hit'
  GROUP BY ATBATS.batter
), outs AS
(
  SELECT ATBATS.batter, count(*) AS num_outs
  FROM ATBATS
  WHERE ATBATS.ab_result = 'out'
  GROUP BY ATBATS.batter
), no_ab AS
(
  SELECT ATBATS.batter, count(*) AS num_no_ab
  FROM ATBATS
  WHERE ATBATS.ab_result = 'no at-bat'
  GROUP BY ATBATS.batter
)
SELECT h.batter,
  1.0 * h.num_hits / (h.num_hits + o.num_outs) AS batting_avg,
  h.num_hits + o.num_outs AS at_bats,
  CASE WHEN n.num_no_ab is NULL THEN h.num_hits + o.num_outs
    ELSE h.num_hits + o.num_outs + n.num_no_ab
  END AS plate_app
FROM hits h JOIN outs o ON h.batter = o.batter
  LEFT JOIN no_ab n on h.batter = n.batter
ORDER BY (h.num_hits / (h.num_hits + o.num_outs)) DESC;
