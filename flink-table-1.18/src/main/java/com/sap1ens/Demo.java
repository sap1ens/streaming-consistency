package com.sap1ens;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Demo {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.setString("rest.port", "8888");
		config.setString("taskmanager.memory.network.max", "1gb");

		EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

		tEnv.executeSql(String.join("\n",
				"CREATE TABLE transactions (",
				"    id  BIGINT,",
				"    from_account INT,",
				"    to_account INT,",
				"    amount DOUBLE,",
				"    ts TIMESTAMP(3),",
				"    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
				") WITH (",
				"    'connector' = 'kafka',",
				"    'topic' = 'transactions',",
				"    'properties.bootstrap.servers' = 'localhost:9092',",
				"    'properties.group.id' = 'demo',",
				"    'scan.startup.mode' = 'earliest-offset',",
				"    'format' = 'json',",
				"    'json.fail-on-missing-field' = 'true',",
				"    'json.ignore-parse-errors' = 'false'",
				")"
		));

		tEnv.executeSql(String.join("\n",
				"CREATE VIEW accepted_transactions(id) AS",
				"SELECT",
				"    id",
				"FROM",
				"    transactions"
		));
		sinkToKafka(tEnv, "accepted_transactions");

		tEnv.executeSql(String.join("\n",
				"CREATE VIEW outer_join_with_time(id, other_id) AS",
				"SELECT",
				"    t1.id, t2.id as other_id",
				"FROM",
				"    transactions as t1",
				"LEFT JOIN",
				"    transactions as t2",
				"ON",
				"    t1.id = t2.id AND t1.ts = t2.ts"
		));
		sinkToKafka(tEnv, "outer_join_with_time");

		tEnv.executeSql(String.join("\n",
				"CREATE VIEW outer_join_without_time(id, other_id) AS",
				"SELECT",
				"    t1.id, t2.id as other_id",
				"FROM",
				"    (SELECT id FROM transactions) as t1",
				"LEFT JOIN",
				"    (SELECT id FROM transactions) as t2",
				"ON",
				"    t1.id = t2.id"
		));
		sinkToKafka(tEnv, "outer_join_without_time");

		tEnv.executeSql(String.join("\n",
				"CREATE VIEW credits(account, credits) AS",
				"SELECT",
				"    to_account as account, sum(amount) as credits",
				"FROM",
				"    transactions",
				"GROUP BY",
				"    to_account"
		));
		sinkToKafka(tEnv, "credits");
		tEnv.executeSql(String.join("\n",
				"CREATE VIEW debits(account, debits) AS",
				"SELECT",
				"    from_account as account, sum(amount) as debits",
				"FROM",
				"    transactions",
				"GROUP BY",
				"    from_account"
		));
		sinkToKafka(tEnv, "debits");
		tEnv.executeSql(String.join("\n",
				"CREATE VIEW balance(account, balance) AS",
				"SELECT",
				"    credits.account, credits - debits as balance",
				"FROM",
				"    credits, debits",
				"WHERE",
				"    credits.account = debits.account"
		));
		sinkToKafka(tEnv, "balance");
		tEnv.executeSql(String.join("\n",
				"CREATE VIEW total(total) AS",
				"SELECT",
				"    sum(balance)",
				"FROM",
				"    balance"
		));
		sinkToKafka(tEnv, "total");

		sEnv.execute("Demo");
	}

	public static void sinkToKafka(StreamTableEnvironment tEnv, String name) {
		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(name)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // TODO: try to use EXACTLY_ONCE
				.build();

		tEnv
			.toRetractStream(tEnv.from(name), Row.class) // TODO: use changelog stream
			.map(kv ->
					(kv.getField(0) ? "insert" : "delete")
							+ " "
							+ kv.getField(1).toString()
			).sinkTo(sink);
	}
}
