-- Prefix with WITH classified AS (...); sub_cent must fail before publication.
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
