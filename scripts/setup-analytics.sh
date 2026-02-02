#!/bin/bash
# scripts/setup-analytics.sh
set -e

KSQL_HOST="http://localhost:8088"
CONNECT_HOST="localhost:8083"

echo "----------------------------------------------------------------"
echo "1. Initializing KSQL Stream Processing"
echo "----------------------------------------------------------------"

# We use the /ksql endpoint to execute lines from the .sql file
# Note: This is a simplified way to submit KSQL. 
# complex logic might require splitting the file.

# Read the SQL file
SQL_CONTENT=$(cat ksql/queries.sql)

# Submit to KSQL
# We submit one by one or as a block. 
# For simplicity in this script, we'll use docker exec to run ksql cli
echo "Submitting KSQL queries..."
docker exec -i ksqldb ksql http://localhost:8088 <<EOF
run script '/data/ksql/queries.sql';
exit;
EOF

echo "----------------------------------------------------------------"
echo "2. Connecting Trending Stream to Elasticsearch"
echo "----------------------------------------------------------------"

curl -i -X PUT -H "Content-Type:application/json" \
    http://$CONNECT_HOST/connectors/sink-elastic-trending/config \
    -d '{
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url": "http://elasticsearch:9200",
        "topics": "trending_keywords",
        "type.name": "_doc",
        "key.ignore": "false", 
        "schema.ignore": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "extractKey",
        "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.extractKey.field": "KEYWORD"
    }'

echo -e "\n\nAnalytics Loop Configured!"
