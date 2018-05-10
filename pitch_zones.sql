DROP MATERIALIZED VIEW IF EXISTS PITCH_ZONES CASCADE;
CREATE MATERIALIZED VIEW PITCH_ZONES AS
SELECT game_id,
  ab_num,
  pitch_id,
  pitcher,
  batter,
  CASE
    WHEN ((px IS NULL) OR (pz IS NULL)) THEN NULL
    WHEN ((px > 0.75) AND (pz > (2.0 * sz_top + sz_bot) / 3.0)) THEN (1)
    WHEN ((px > 0.75) AND (pz < (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz > (sz_top + 2.0 * sz_bot) / 3.0))
      THEN (2)
    WHEN ((px > 0.75) AND (pz < (sz_top + 2.0 * sz_bot) / 3.0)) THEN (3)
    WHEN ((px < 0.75) AND (px > -0.75) AND (pz < sz_bot)) THEN (4)
    WHEN ((px < -0.75) AND (pz < (sz_top + 2.0 * sz_bot) / 3.0)) THEN (5)
    WHEN ((px < -0.75) AND (pz < (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz > (sz_top + 2.0 * sz_bot) / 3.0))
      THEN (6)
    WHEN ((pz < -0.75) AND (pz > ( 2.0 * sz_top +sz_bot) / 3.0)) THEN (7)
    WHEN ((px < 0.75) AND (px > -0.75) AND (pz > sz_top)) THEN (8)
    WHEN ((px >= 0.0) AND (pz > (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz <= sz_top)) THEN (9)
    WHEN ((px >= 0.0) AND (pz <= (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz > (sz_top + 2.0 * sz_bot) / 3.0))
      THEN (10)
    WHEN ((px >= 0.0) AND (pz <= (sz_top + 2.0 * sz_bot) / 3.0)
          AND (pz >= sz_bot)) THEN (11)
    WHEN ((px < 0.0) AND (pz <= (sz_top + 2.0 * sz_bot) / 3.0)
          AND (pz >= sz_bot)) THEN (12)
    WHEN ((px < 0.0) AND (pz <= (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz > (sz_top + 2.0 * sz_bot) / 3.0))
      THEN (13)
    WHEN ((px < 0.0) AND (pz > (2.0 * sz_top + sz_bot) / 3.0)
          AND (pz <= sz_top) THEN (14)
    ELSE (14)
  END AS pitch_zone,
FROM pitches_abs
ORDER BY game_id, ab_num, pitch_id;
