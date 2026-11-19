# --------------------------------------------------------
# Least-privilege identities for the self-managed Kafka Connect side
#
#   worker    the two Connect workers (internal + license topics, worker groups); no access to table data
#             or Schema Registry
#   source    Debezium SQL Server source : WRITE cdc.* topics and the schema history topic,
#                                          WRITE (register) cdc.* subjects in Schema Registry
#   pgsink    PostgreSQL sink            : READ  cdc.* topics, its consumer group, READ cdc.* subjects
#   db2sink   Db2 sink                   : same as pgsink, own consumer group
#
# Each account has its own Kafka API key and its own Schema Registry API key. The connectors
# pick theirs via producer/consumer.override.* and their own converters (see gen_connectors.py).
# --------------------------------------------------------
locals {
  topic_prefix = "cdc."

  connect_identities = {
    source  = { description = "Debezium SQL Server CDC source connector (read/write)" }
    pgsink  = { description = "PostgreSQL JDBC sink connector (read only)" }
    db2sink = { description = "Db2 JDBC sink connector (read only)" }
  }

  kafka_crn = "${confluent_kafka_cluster.cc_kafka_cluster.rbac_crn}/kafka=${confluent_kafka_cluster.cc_kafka_cluster.id}"
  sr_crn    = data.confluent_schema_registry_cluster.cc_sr_cluster.resource_name

  # sink consumer groups are connect-<connector name> (see gen_connectors.py)
  sink_groups = {
    pgsink  = "connect-postgres-sink"
    db2sink = "connect-db2-sink"
  }
}

resource "confluent_service_account" "connect_worker" {
  display_name = "connect-worker-${random_id.id.hex}"
  description  = "Self-managed Kafka Connect workers (internal topics and worker groups only)"
}

resource "confluent_service_account" "connector" {
  for_each     = local.connect_identities
  display_name = "${each.key}-${random_id.id.hex}"
  description  = each.value.description
}

# ---------------------------------------------------------------- Connect worker (Kafka)
resource "confluent_role_binding" "connect_kafka" {
  principal   = "User:${confluent_service_account.connect_worker.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${local.kafka_crn}/topic=docker-connect-*"
}
resource "confluent_role_binding" "connect_license_topic" {
  principal   = "User:${confluent_service_account.connect_worker.id}"
  role_name   = "ResourceOwner" # the worker's license check reads/writes _confluent-command
  crn_pattern = "${local.kafka_crn}/topic=_confluent-command"
}
resource "confluent_role_binding" "connect_groups" {
  principal   = "User:${confluent_service_account.connect_worker.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${local.kafka_crn}/group=db-replication-connect-*"
}
# ---------------------------------------------------------------- Source: read/write
resource "confluent_role_binding" "source_topics" {
  principal   = "User:${confluent_service_account.connector["source"].id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${local.kafka_crn}/topic=${local.topic_prefix}*"
}
resource "confluent_role_binding" "source_schema_history" {
  principal   = "User:${confluent_service_account.connector["source"].id}"
  role_name   = "ResourceOwner" # reads and writes (and describes) its own history topic
  crn_pattern = "${local.kafka_crn}/topic=mssql.schema-history"
}
# Debezium reads the schema history back with consumer group <topic.prefix>-schemahistory when it
# restarts with existing offsets (redeploy, worker restart): without this the task fails with
# GroupAuthorizationException
resource "confluent_role_binding" "source_schema_history_group" {
  principal   = "User:${confluent_service_account.connector["source"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=mssql-schemahistory"
}
resource "confluent_role_binding" "source_subjects" {
  principal   = "User:${confluent_service_account.connector["source"].id}"
  role_name   = "DeveloperWrite" # register schemas (and read them back)
  crn_pattern = "${local.sr_crn}/subject=${local.topic_prefix}*"
}

# ---------------------------------------------------------------- Sinks: read only
resource "confluent_role_binding" "sink_topics" {
  for_each    = local.sink_groups
  principal   = "User:${confluent_service_account.connector[each.key].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/topic=${local.topic_prefix}*"
}
resource "confluent_role_binding" "sink_group" {
  for_each    = local.sink_groups
  principal   = "User:${confluent_service_account.connector[each.key].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=${each.value}"
}
resource "confluent_role_binding" "sink_subjects" {
  for_each    = local.sink_groups
  principal   = "User:${confluent_service_account.connector[each.key].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.sr_crn}/subject=${local.topic_prefix}*"
}

# ---------------------------------------------------------------- API keys
resource "confluent_api_key" "connect_kafka" {
  display_name = "connect-worker-kafka-key-${random_id.id.hex}"
  description  = "Kafka API key of the Connect workers"
  owner {
    id          = confluent_service_account.connect_worker.id
    api_version = confluent_service_account.connect_worker.api_version
    kind        = confluent_service_account.connect_worker.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cc_kafka_cluster.id
    api_version = confluent_kafka_cluster.cc_kafka_cluster.api_version
    kind        = confluent_kafka_cluster.cc_kafka_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.connect_kafka, confluent_role_binding.connect_groups, confluent_role_binding.connect_license_topic]
}

resource "confluent_api_key" "connector_kafka" {
  for_each     = local.connect_identities
  display_name = "${each.key}-kafka-key-${random_id.id.hex}"
  description  = "Kafka API key of ${each.key}"
  owner {
    id          = confluent_service_account.connector[each.key].id
    api_version = confluent_service_account.connector[each.key].api_version
    kind        = confluent_service_account.connector[each.key].kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cc_kafka_cluster.id
    api_version = confluent_kafka_cluster.cc_kafka_cluster.api_version
    kind        = confluent_kafka_cluster.cc_kafka_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [
    confluent_role_binding.source_topics, confluent_role_binding.source_schema_history, confluent_role_binding.source_schema_history_group,
    confluent_role_binding.sink_topics, confluent_role_binding.sink_group,
  ]
}

resource "confluent_api_key" "connector_sr" {
  for_each     = local.connect_identities
  display_name = "${each.key}-sr-key-${random_id.id.hex}"
  description  = "Schema Registry API key of ${each.key}"
  owner {
    id          = confluent_service_account.connector[each.key].id
    api_version = confluent_service_account.connector[each.key].api_version
    kind        = confluent_service_account.connector[each.key].kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.cc_sr_cluster.id
    api_version = data.confluent_schema_registry_cluster.cc_sr_cluster.api_version
    kind        = data.confluent_schema_registry_cluster.cc_sr_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.source_subjects, confluent_role_binding.sink_subjects]
}

# Everything the compose file needs, keyed by identity (worker, source, pgsink, db2sink)
output "connect_credentials" {
  sensitive = true
  value = merge(
    { worker = {
      kafka_key = confluent_api_key.connect_kafka.id, kafka_secret = confluent_api_key.connect_kafka.secret
    } },
    { for k in keys(local.connect_identities) : k => {
      kafka_key = confluent_api_key.connector_kafka[k].id, kafka_secret = confluent_api_key.connector_kafka[k].secret,
      sr_key    = confluent_api_key.connector_sr[k].id, sr_secret = confluent_api_key.connector_sr[k].secret
    } }
  )
}
