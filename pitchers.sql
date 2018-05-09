DROP MATERIALIZED VIEW IF EXISTS PITCHERS CASCADE;
CREATE MATERIALIZED VIEW PITCHERS AS
WITH hits AS
(
  SELECT ATBATS.pitcher,count(*) AS num_hits
  FROM ATBATS
  WHERE ATBATS.ab_result = 'hit'
  GROUP BY ATBATS.pitcher
), outs AS
(
  SELECT ATBATS.pitcher,count(*) AS num_outs
  FROM ATBATS
  WHERE ATBATS.ab_result = 'out'
  GROUP BY ATBATS.pitcher
), o_avg AS
(
  SELECT h.pitcher,
    1.0 * h.num_hits / (h.num_hits + o.num_outs) AS opp_ba,
    h.num_hits + o.num_outs AS at_bats
  FROM hits h JOIN outs o ON h.pitcher = o.pitcher
), balls AS
(
  SELECT pitcher,count(*) AS num_balls
  FROM pitches_abs
  WHERE pitch_result = 'B'
  GROUP BY pitcher
), strikes AS
(
  SELECT pitcher,count(*) AS num_strikes
  FROM pitches_abs
  WHERE pitch_result = 'S'
  GROUP BY pitcher
), b_outs AS
(
  SELECT pitcher,count(*) AS num_b_outs
  FROM pitches_abs
  WHERE pitch_result = 'X'
  GROUP BY pitcher
), p_hits AS
(
  SELECT pitcher,count(*) AS num_hits
  FROM pitches_abs
  WHERE pitch_result = 'H'
  GROUP BY pitcher
), summary AS
(
  SELECT o_avg.pitcher,
    o_avg.opp_ba,
    o_avg.at_bats,
    b.num_balls,
    st.num_strikes,
    b_outs.num_b_outs,
    p_hits.num_hits
  FROM o_avg JOIN balls b ON o_avg.pitcher = b.pitcher
    JOIN strikes st ON o_avg.pitcher = st.pitcher
    JOIN b_outs ON o_avg.pitcher = b_outs.pitcher
    JOIN p_hits ON o_avg.pitcher = p_hits.pitcher
), p_stats AS
(
  SELECT pitcher,
    count(*) AS num_pitches,
    avg(start_speed) AS mean_sp,
    stddev(start_speed) AS std_sp,
    avg(pz) AS mean_y,
    avg(px) AS mean_x,
    avg(pfx_z) AS mean_dy,
    avg(pfx_x) AS mean_dx,
    stddev(pz) AS std_y,
    stddev(px) AS std_x,
    stddev(pfx_z) AS std_dy,
    stddev(pfx_x) AS std_dx
  FROM pitches_abs
  GROUP BY pitcher
)
SELECT ps.*,
       s.opp_ba,
       s.at_bats,
       s.num_balls,
       s.num_strikes,
       s.num_b_outs,
       s.num_hits
FROM p_stats ps JOIN summary s ON ps.pitcher = s.pitcher
ORDER BY ps.pitcher;
