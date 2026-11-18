"""
Generates the connector configs for the Confluent Cloud variant (run from anywhere):

    python3 confluent_cloud/scripts/gen_connectors.py

Same connectors as the local demo (the source runs on its own Connect cluster, the two sinks on another;
topics are named cdc.<table>) (Debezium SQL Server source + 2 JDBC sinks, same
SMTs, same upsert / delete-on-tombstone settings), with two differences:

* Kafka / Schema Registry credentials are Confluent Cloud ones, one service account per
  connector (source read/write, sinks read only). They are NOT written to
  these files: values like ${env:CC_SOURCE_KAFKA_API_KEY} are resolved by the Connect worker's
  EnvVarConfigProvider from the container environment (filled from cloud.env), so the
  JSON files contain no secrets and the REST API never returns them.
* topics are pre-created by terraform (compacted), so no topic.creation.* settings.
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "connectors")
TABLES = [t.strip() for t in open(os.path.join(HERE, "..", "..", "scripts", "tables.txt")) if t.strip()]

def jaas(ident):
    """SASL/PLAIN login of one service account, resolved from the worker environment (cloud.env)."""
    return ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="${{env:CC_{ident}_KAFKA_API_KEY}}" password="${{env:CC_{ident}_KAFKA_API_SECRET}}";')


def avro(prefix, ident):
    sr_user_info = f"${{env:CC_{ident}_SR_API_KEY}}:${{env:CC_{ident}_SR_API_SECRET}}"
    return {
        f"{prefix}.converter": "io.confluent.connect.avro.AvroConverter",
        f"{prefix}.converter.schema.registry.url": "${env:CC_SR_URL}",
        f"{prefix}.converter.basic.auth.credentials.source": "USER_INFO",
        f"{prefix}.converter.basic.auth.user.info": sr_user_info,
        f"{prefix}.converter.schema.registry.basic.auth.user.info": sr_user_info,
    }


def kafka_client(prefix, ident):
    """Security settings for a Kafka client created *by the connector itself* (Debezium schema history)."""
    return {
        f"{prefix}.security.protocol": "SASL_SSL",
        f"{prefix}.sasl.mechanism": "PLAIN",
        f"{prefix}.sasl.jaas.config": jaas(ident),
    }


source = {
    "name": "mssql-cdc-source",
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "tasks.max": "1",
        "database.hostname": "mssql",
        "database.port": "1433",
        "database.user": "sa",
        "database.password": "Password!123",
        "database.names": "demo_db",
        "database.encrypt": "false",
        "database.trustServerCertificate": "true",
        "topic.prefix": "mssql",
        "table.include.list": ",".join(f"dbo.{n}" for n in TABLES),
        "snapshot.mode": "initial",
        # Debezium schema history topic lives in Confluent Cloud too
        "schema.history.internal.kafka.bootstrap.servers": "${env:CC_KAFKA_BOOTSTRAP}",
        "schema.history.internal.kafka.topic": "mssql.schema-history",
        **kafka_client("schema.history.internal.producer", "SOURCE"),
        **kafka_client("schema.history.internal.consumer", "SOURCE"),
        **kafka_client("schema.history.internal.admin", "SOURCE"),
        "include.schema.changes": "false",
        "time.precision.mode": "connect",
        "decimal.handling.mode": "precise",
        "tombstones.on.delete": "true",
        # the connector's own service account (read/write) instead of the worker's
        **kafka_client("producer.override", "SOURCE"),
        **avro("key", "SOURCE"),
        **avro("value", "SOURCE"),
        # "after state only": flat row, delete event dropped, tombstone kept
        "transforms": "unwrap,route",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "drop",
        # mssql.demo_db.dbo.customers -> cdc.customers
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "mssql\\.demo_db\\.dbo\\.(.*)",
        "transforms.route.replacement": "cdc.$1",
    },
}


def sink(name, ident, url, user, password):
    return {
        "name": name,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "2",
            "topics": ",".join(f"cdc.{t}" for t in TABLES),
            "connection.url": url,
            "connection.user": user,
            "connection.password": password,
            # the connector's own service account (read only); consumer group = connect-<name>
            **kafka_client("consumer.override", ident),
            **avro("key", ident),
            **avro("value", ident),
            # the target tables have no prefix: cdc.customers -> customers
            "transforms": "dropPrefix",
            "transforms.dropPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.dropPrefix.regex": "cdc\\.(.*)",
            "transforms.dropPrefix.replacement": "$1",
            "table.name.format": "${topic}",
            "pk.mode": "record_key",
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "auto.create": "false",
            "auto.evolve": "false",
        },
    }


def write(name, obj):
    path = os.path.join(OUT, name)
    os.makedirs(OUT, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")
    print("wrote", os.path.relpath(path, os.path.join(HERE, "..", "..")))


write("mssql-source-connector.json", source)
write("postgres-sink-connector.json", sink("postgres-sink", "PGSINK", "jdbc:postgresql://postgres:5432/demo", "postgres", "postgres"))
write("db2-sink-connector.json", sink("db2-sink", "DB2SINK", "jdbc:db2://db2:50000/demo", "db2inst1", "db2inst1-pwd"))
