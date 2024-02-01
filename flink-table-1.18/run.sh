#!/usr/bin/env bash

set -ue

echo "Deleting topics"
delete_topic() {
    kafka-topics --delete \
        --bootstrap-server localhost:9092 \
        --topic "$1"
}
delete_topic transactions
delete_topic accepted_transactions
delete_topic outer_join_with_time
delete_topic outer_join_without_time
delete_topic credits
delete_topic debits
delete_topic balance
delete_topic total

echo "Creating topics"
create_topic() {
    kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --config retention.ms=-1 \
        --topic "$1"
}
create_topic transactions
create_topic accepted_transactions
create_topic outer_join_with_time
create_topic outer_join_without_time
create_topic credits
create_topic debits
create_topic balance
create_topic total


echo "Feeding inputs"
cat transactions.txt | kafka-console-producer \
    --broker-list localhost:9092 \
    --topic transactions \
    --property "key.separator=|" \
    --property "parse.key=true"

rm -rf ./tmp
mkdir -p ./tmp

echo "Watching outputs"
watch_topic() {
    kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$1" \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        > "./tmp/$1" &
}
watch_topic transactions
watch_topic accepted_transactions
watch_topic outer_join_with_time
watch_topic outer_join_without_time
watch_topic credits
watch_topic debits
watch_topic balance
watch_topic total

# run "pkill -f ConsoleConsumer" for killing all consumers
