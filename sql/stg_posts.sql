DROP TABLE IF EXISTS stg_posts;

CREATE TABLE stg_posts AS
SELECT
    id,
    user_id,
    LOWER(title) AS title,
    body,
    LENGTH(body) AS body_length
FROM raw_posts;