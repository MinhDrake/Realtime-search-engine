-- 1. Create a Stream for raw search queries
CREATE STREAM search_stream (
    keyword VARCHAR,
    ts BIGINT
) WITH (
    KAFKA_TOPIC = 'search.queries',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'ts'
);

-- 2. Aggregate: Count keywords in 60-second windows
-- Output to topic 'trending_keywords'
CREATE TABLE trending_keywords WITH (
    KAFKA_TOPIC = 'trending_keywords',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
) AS
SELECT
    keyword,
    COUNT(*) AS count,
    WINDOWSTART as window_start,
    WINDOWEND as window_end
FROM search_stream
WINDOW TUMBLING (SIZE 60 SECONDS)
GROUP BY keyword
EMIT CHANGES;
