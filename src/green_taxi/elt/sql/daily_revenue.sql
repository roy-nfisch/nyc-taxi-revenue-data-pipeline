-- Prefix with WITH classified AS (...) containing per-source classifications.
-- Build one fact row per accepted pickup day: count rides and add their cents.
-- Example: two accepted rides worth 125 and 275 cents produce count 2, total 400.
-- Invalid/out-of-period rides do not contribute. Accepted negative amounts do.
-- No accepted rides on a day means no fact row; the separate date dimension
-- supplies the full calendar. Keep cents exact until the report adapter formats them.
SELECT service_date, count(*) AS trip_count, sum(amount_cents) AS revenue_cents
FROM classified
WHERE status = 'accepted'
GROUP BY service_date
ORDER BY service_date
