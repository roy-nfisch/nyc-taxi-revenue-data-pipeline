-- Prefix with WITH classified AS (...); sub_cent must fail before publication.
-- Return one summary row for checking and reporting what happened to the input.
-- FILTER counts only rows meeting its condition; it does not change the input.
-- rows_read includes every classification. rows_used counts accepted rides.
-- The invalid/outside/sub_cent status counts explain why other rides were not used.
-- Negative and zero counts are subsets of rows_used, not additional exclusions.
-- The caller must reject any sub_cent result before saving a successful report.
SELECT
    count(*) AS rows_read,
    count(*) FILTER (WHERE status = 'accepted') AS rows_used,
    count(*) FILTER (WHERE status = 'invalid_pickup') AS invalid_pickup,
    count(*) FILTER (WHERE status = 'outside_period') AS outside_period,
    count(*) FILTER (WHERE status = 'invalid_amount') AS invalid_amount,
    count(*) FILTER (WHERE status = 'accepted' AND amount_cents < 0::BIGNUM) AS negative_amount,
    count(*) FILTER (WHERE status = 'accepted' AND amount_cents = 0::BIGNUM) AS zero_amount,
    count(*) FILTER (WHERE status = 'sub_cent') AS sub_cent
FROM classified
