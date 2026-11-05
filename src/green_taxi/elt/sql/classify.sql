-- Classify each ride before calculating revenue. Keep one output row per input
-- row, including duplicates: this query labels records; it does not remove them.
-- Output: service_date, status and exact signed amount_cents (NULL if rejected).
--
-- The caller replaces __SOURCE_TABLE__ with one validated, quoted
-- raw.source_YYYY_MM identifier. Values use bound parameters, not SQL text:
-- period_start is included; period_end is excluded. pickup_kind and amount_kind
-- describe the original column types so booleans or aware timestamps cannot
-- become valid money or dates just because their text looks usable.
--
-- WITH names below are intermediate query results, not saved database tables.
-- First check the pickup, then the period, then the money, matching ETL rules.
-- Keep all parsing in SQL; do not coerce raw sources into one common schema.
WITH raw_text AS (
    -- Read a single source and prepare text for parsing without rounding money.
    SELECT
        CAST(lpep_pickup_datetime AS VARCHAR) AS pickup_text,
        CASE
            WHEN $amount_kind NOT IN ('numeric', 'text') THEN NULL
            -- pandas exposes Arrow float32 values as Python float (binary64).
            WHEN typeof(total_amount) = 'FLOAT'
                THEN CAST(TRY_CAST(total_amount AS DOUBLE) AS VARCHAR)
            ELSE CAST(total_amount AS VARCHAR)
        END AS money_text
    FROM __SOURCE_TABLE__
), pickup_boundary AS (
    -- Find where the date ends. Supported ISO dates can use calendar or week
    -- notation and may omit dashes, so the date is not always 10 characters.
    SELECT *,
        -- Follow fromisoformat's separator precedence for ambiguous week dates.
        CASE
            WHEN length(pickup_text) = 7 THEN 7
            WHEN substring(pickup_text, 5, 1) = '-' THEN
                CASE WHEN substring(pickup_text, 6, 1) = 'W' THEN
                    CASE WHEN substring(pickup_text, 9, 1) = '-' THEN
                        CASE WHEN regexp_full_match(substring(pickup_text, 11, 1), '[0-9]')
                            THEN 8 ELSE 10 END
                    ELSE 8 END
                ELSE 10 END
            WHEN substring(pickup_text, 5, 1) = 'W' THEN
                CASE
                    WHEN length(regexp_extract(substring(pickup_text, 8), '^[0-9]*')) < 2
                        THEN 7 + length(regexp_extract(substring(pickup_text, 8), '^[0-9]*'))
                    WHEN length(regexp_extract(substring(pickup_text, 8), '^[0-9]*')) % 2 = 1
                        THEN 7
                    ELSE 8
                END
            ELSE 8
        END AS date_length
    FROM raw_text
), pickup_parts AS (
    -- Separate the date from any following time.
    SELECT *, substring(pickup_text, 1, date_length) AS date_part
    FROM pickup_boundary
), pickup_clock AS (
    -- Prepare the clock and a dash-free date for the checks below.
    SELECT *,
        -- fromisoformat accepts any ONE separator, not just a space or T.
        substring(pickup_text, length(date_part) + 2) AS clock_part,
        replace(date_part, '-', '') AS compact_date
    FROM pickup_parts
), pickup_candidates AS (
    -- Attempt a calendar date, including conversion from ISO year/week/day.
    -- TRY_CAST and try_strptime return NULL for invalid input instead of raising.
    SELECT *,
        CASE
            WHEN contains(compact_date, 'W') THEN
                TRY_CAST(try_strptime(
                    substring(compact_date, 1, 4) || '-W'
                    || substring(compact_date, 6, 2) || '-'
                    || coalesce(nullif(substring(compact_date, 8, 1), ''), '1'),
                    '%G-W%V-%u') AS DATE)
            ELSE TRY_CAST(substring(compact_date, 1, 4) || '-'
                || substring(compact_date, 5, 2) || '-'
                || substring(compact_date, 7, 2) AS DATE)
        END AS candidate,
        replace(regexp_replace(clock_part, '[.,][0-9]+$', ''), ':', '') AS clock_digits
    FROM pickup_clock
), dated AS (
    -- Accept only supported original types or valid ISO text. Validate the clock
    -- too: a valid-looking date must not hide an impossible time. Return the
    -- recorded pickup day, with no timezone conversion; unsupported types stay NULL.
    SELECT money_text,
        CASE
            WHEN $pickup_kind IN ('date', 'timestamp_naive') THEN
                CASE WHEN year(TRY_CAST(pickup_text AS DATE)) BETWEEN 1 AND 9999
                    THEN TRY_CAST(pickup_text AS DATE) END
            WHEN $pickup_kind = 'text'
                AND regexp_full_match(date_part,
                    '([0-9]{4}-W[0-9]{2}(-[1-7])?|[0-9]{4}W[0-9]{2}[1-7]?|'
                    || '[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8})')
                AND year(candidate) BETWEEN 1 AND 9999
                AND (NOT contains(compact_date, 'W')
                    OR strftime(candidate, '%G%V%u') =
                        replace(compact_date, 'W', '')
                        || CASE WHEN length(compact_date) = 7 THEN '1' ELSE '' END)
                AND (length(pickup_text) = length(date_part)
                    OR (
                        length(pickup_text) > length(date_part) + 1
                        AND regexp_full_match(clock_part,
                            '(([0-9]{2}(:[0-9]{2}(:[0-9]{2})?)?|'
                            || '[0-9]{4}([0-9]{2})?)([.,][0-9]+)?|'
                            -- CPython also accepts these fractional-second forms.
                            || '[0-9]{8,}|[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]+)')
                        AND TRY_CAST(substring(clock_digits, 1, 2) AS INTEGER) < 24
                        AND coalesce(TRY_CAST(substring(clock_digits, 3, 2) AS INTEGER), 0) < 60
                        AND coalesce(TRY_CAST(substring(clock_digits, 5, 2) AS INTEGER), 0) < 60
                    ))
                THEN candidate
        END AS service_date
    FROM pickup_candidates
), eligible AS (
    -- Date failures take priority over money failures. A ride outside the chosen
    -- period is counted as outside_period even if its amount is also invalid.
    SELECT service_date,
        CASE
            WHEN service_date IS NULL THEN 'invalid_pickup'
            WHEN service_date < $period_start OR service_date >= $period_end
                THEN 'outside_period'
        END AS pickup_status,
        -- An excluded pickup's money is not parsed, even if it is sub-cent.
        CASE WHEN service_date >= $period_start AND service_date < $period_end
            THEN money_text END AS money_text
    FROM dated
), money_normalized AS (
    -- Match the existing Python Decimal text rules, not a new cleanup policy.
    -- For example, surrounding whitespace and Unicode decimal digits are valid.
    SELECT service_date, pickup_status,
        -- Decimal(str(value)) strips Python whitespace and ignores underscores.
        -- translate covers Python 3.13's Unicode 15.1 decimal digits (Nd).
        translate(replace(trim(money_text,
            chr(9) || chr(10) || chr(11) || chr(12) || chr(13)
            || chr(28) || chr(29) || chr(30) || chr(31) || ' '
            || chr(133) || chr(160) || chr(5760)
            || chr(8192) || chr(8193) || chr(8194) || chr(8195) || chr(8196)
            || chr(8197) || chr(8198) || chr(8199) || chr(8200) || chr(8201)
            || chr(8202) || chr(8232) || chr(8233) || chr(8239)
            || chr(8287) || chr(12288)), '_', ''),
            '0123456789٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹߀߁߂߃߄߅߆߇߈߉०१२३४५६७८९০১২৩৪৫৬৭৮৯੦੧੨੩੪੫੬੭੮੯૦૧૨૩૪૫૬૭૮૯୦୧୨୩୪୫୬୭୮୯௦௧௨௩௪௫௬௭௮௯' ||
            '౦౧౨౩౪౫౬౭౮౯೦೧೨೩೪೫೬೭೮೯൦൧൨൩൪൫൬൭൮൯෦෧෨෩෪෫෬෭෮෯๐๑๒๓๔๕๖๗๘๙໐໑໒໓໔໕໖໗໘໙༠༡༢༣༤༥༦༧༨༩၀၁၂၃၄၅၆၇၈၉႐႑႒႓႔႕႖႗႘႙០១២៣៤៥៦៧៨៩' ||
            '᠐᠑᠒᠓᠔᠕᠖᠗᠘᠙᥆᥇᥈᥉᥊᥋᥌᥍᥎᥏᧐᧑᧒᧓᧔᧕᧖᧗᧘᧙᪀᪁᪂᪃᪄᪅᪆᪇᪈᪉᪐᪑᪒᪓᪔᪕᪖᪗᪘᪙᭐᭑᭒᭓᭔᭕᭖᭗᭘᭙᮰᮱᮲᮳᮴᮵᮶᮷᮸᮹᱀᱁᱂᱃᱄᱅᱆᱇᱈᱉᱐᱑᱒᱓᱔᱕᱖᱗᱘᱙꘠꘡꘢꘣꘤꘥꘦꘧꘨꘩' ||
            '꣐꣑꣒꣓꣔꣕꣖꣗꣘꣙꤀꤁꤂꤃꤄꤅꤆꤇꤈꤉꧐꧑꧒꧓꧔꧕꧖꧗꧘꧙꧰꧱꧲꧳꧴꧵꧶꧷꧸꧹꩐꩑꩒꩓꩔꩕꩖꩗꩘꩙꯰꯱꯲꯳꯴꯵꯶꯷꯸꯹０１２３４５６７８９𐒠𐒡𐒢𐒣𐒤𐒥𐒦𐒧𐒨𐒩𐴰𐴱𐴲𐴳𐴴𐴵𐴶𐴷𐴸𐴹𑁦𑁧𑁨𑁩𑁪𑁫𑁬𑁭𑁮𑁯' ||
            '𑃰𑃱𑃲𑃳𑃴𑃵𑃶𑃷𑃸𑃹𑄶𑄷𑄸𑄹𑄺𑄻𑄼𑄽𑄾𑄿𑇐𑇑𑇒𑇓𑇔𑇕𑇖𑇗𑇘𑇙𑋰𑋱𑋲𑋳𑋴𑋵𑋶𑋷𑋸𑋹𑑐𑑑𑑒𑑓𑑔𑑕𑑖𑑗𑑘𑑙𑓐𑓑𑓒𑓓𑓔𑓕𑓖𑓗𑓘𑓙𑙐𑙑𑙒𑙓𑙔𑙕𑙖𑙗𑙘𑙙𑛀𑛁𑛂𑛃𑛄𑛅𑛆𑛇𑛈𑛉𑜰𑜱𑜲𑜳𑜴𑜵𑜶𑜷𑜸𑜹𑣠𑣡𑣢𑣣𑣤𑣥𑣦𑣧𑣨𑣩' ||
            '𑥐𑥑𑥒𑥓𑥔𑥕𑥖𑥗𑥘𑥙𑱐𑱑𑱒𑱓𑱔𑱕𑱖𑱗𑱘𑱙𑵐𑵑𑵒𑵓𑵔𑵕𑵖𑵗𑵘𑵙𑶠𑶡𑶢𑶣𑶤𑶥𑶦𑶧𑶨𑶩𑽐𑽑𑽒𑽓𑽔𑽕𑽖𑽗𑽘𑽙𖩠𖩡𖩢𖩣𖩤𖩥𖩦𖩧𖩨𖩩𖫀𖫁𖫂𖫃𖫄𖫅𖫆𖫇𖫈𖫉𖭐𖭑𖭒𖭓𖭔𖭕𖭖𖭗𖭘𖭙𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗𝟘𝟙𝟚𝟛𝟜𝟝𝟞𝟟𝟠𝟡' ||
            '𝟢𝟣𝟤𝟥𝟦𝟧𝟨𝟩𝟪𝟫𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𞅀𞅁𞅂𞅃𞅄𞅅𞅆𞅇𞅈𞅉𞋰𞋱𞋲𞋳𞋴𞋵𞋶𞋷𞋸𞋹𞓰𞓱𞓲𞓳𞓴𞓵𞓶𞓷𞓸𞓹𞥐𞥑𞥒𞥓𞥔𞥕𞥖𞥗𞥘𞥙🯰🯱🯲🯳🯴🯵🯶🯷🯸🯹',
            repeat('0123456789', 68)) AS amount_text
    FROM eligible
), money_parts AS (
    -- Split scientific notation into its number part (mantissa) and exponent:
    -- 1.25e2 means 1.25 * 10^2. Reject non-numeric text and non-finite values.
    SELECT *,
        regexp_full_match(amount_text,
            '[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?') AS valid_syntax,
        regexp_extract(amount_text, '^[+-]?([^eE]+)', 1) AS mantissa,
        coalesce(TRY_CAST(nullif(regexp_extract(amount_text,
            '[eE]([+-]?[0-9]+)$', 1), '') AS BIGINT), 0) AS exponent,
        nullif(regexp_extract(amount_text, '[eE]([+-]?[0-9]+)$', 1), '') AS exponent_text
    FROM money_normalized
), coefficient AS (
    -- Remove the decimal point, retaining how many fractional places it had.
    -- This lets later steps move digits rather than multiply floating-point money.
    SELECT *,
        ltrim(replace(mantissa, '.', ''), '0') AS digits,
        CASE WHEN contains(mantissa, '.') THEN
            length(split_part(mantissa, '.', 2)) ELSE 0 END AS fractional_digits,
        exponent_text IS NULL OR TRY_CAST(exponent_text AS BIGINT) IS NOT NULL AS parsed_exponent
    FROM money_parts
), checked_exponent AS (
    -- Keep the same representable exponent limits as Python Decimal.
    SELECT *,
        -- CPython Decimal bounds the stored exponent and adjusted exponent,
        -- not the textual exponent before subtracting fractional places.
        parsed_exponent
            AND exponent::HUGEINT - fractional_digits >= -1999999999999999997
            AND exponent::HUGEINT - fractional_digits
                + greatest(length(digits) - 1, 0) <= 999999999999999999 AS valid_exponent
    FROM coefficient
), cents_parts AS (
    -- Convert dollars to cents by shifting the digits two places. Trailing zeros
    -- do not add precision: 1.230 is 123 cents, but 1.231 contains a fraction of
    -- a cent. After removing trailing zeros, a negative scale marks that case.
    SELECT *,
        rtrim(digits, '0') AS significant_digits,
        CASE WHEN valid_exponent THEN exponent ELSE 0 END
            - fractional_digits + length(digits) - length(rtrim(digits, '0')) + 2 AS scale,
        -- Operational guard, not a money/exclusion rule: fail before allocation.
        100000 AS max_cents_digits
    FROM checked_exponent
), classified AS (
    -- Every ride gets one status. sub_cent is a run-blocking error for the
    -- caller, not a record to silently round or discard. Accepted zero and
    -- negative charges remain valid. BIGNUM holds cents as exact integers.
    SELECT service_date,
        CASE
            WHEN pickup_status IS NOT NULL THEN pickup_status
            WHEN NOT coalesce(valid_syntax AND valid_exponent, false) THEN 'invalid_amount'
            WHEN digits != '' AND scale < 0 THEN 'sub_cent'
            ELSE 'accepted'
        END AS status,
        CASE
            WHEN pickup_status IS NOT NULL
                OR NOT coalesce(valid_syntax AND valid_exponent, false) THEN NULL
            WHEN digits = '' THEN '0'::BIGNUM
            WHEN scale >= 0 AND length(significant_digits) + scale > max_cents_digits
                THEN error('Exact-cent SQL resource limit (100000 digits) exceeded.')
            WHEN scale >= 0 THEN CAST(
                CASE WHEN starts_with(amount_text, '-') THEN '-' ELSE '' END
                || significant_digits || repeat('0', scale) AS BIGNUM)
        END AS amount_cents
    FROM cents_parts
)
-- Return classifications for the aggregation and counter queries.
SELECT service_date, status, amount_cents FROM classified

