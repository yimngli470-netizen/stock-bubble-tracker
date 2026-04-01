-- Remove rows that were incorrectly backfilled with 3/31 or 3/30 data
-- for dates 2025-03-17 through 2025-03-30.
-- Sentiment and valuation already skip historical dates, so no cleanup needed there.

DELETE FROM track_deviation  WHERE date >= '2026-03-17' AND date <= '2026-03-30';
DELETE FROM track_liquidity  WHERE date >= '2026-03-17' AND date <= '2026-03-30';
DELETE FROM track_ipo_heat   WHERE date >= '2026-03-17' AND date <= '2026-03-30';
DELETE FROM track_volatility WHERE date >= '2026-03-17' AND date <= '2026-03-30';

-- Verify remaining rows
SELECT 'deviation'  AS tbl, date FROM track_deviation  WHERE date >= '2026-03-01' ORDER BY date;
SELECT 'liquidity'  AS tbl, date FROM track_liquidity  WHERE date >= '2026-03-01' ORDER BY date;
SELECT 'ipo_heat'   AS tbl, date FROM track_ipo_heat   WHERE date >= '2026-03-01' ORDER BY date;
SELECT 'volatility' AS tbl, date FROM track_volatility WHERE date >= '2026-03-01' ORDER BY date;
