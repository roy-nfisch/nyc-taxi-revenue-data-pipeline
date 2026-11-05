-- Prefix with WITH classified AS (...) containing per-source classifications.
SELECT service_date, count(*) AS trip_count, sum(amount_cents) AS revenue_cents
FROM classified
WHERE status = 'accepted'
GROUP BY service_date
ORDER BY service_date
