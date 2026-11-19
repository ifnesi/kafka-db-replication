# --------------------------------------------------------
# Kafbat UI (DevOps console): its own service account, separate from the connectors'
#
#   Kafka           READ (consume, describe) every topic and consumer group, for browsing and lag
#                   WRITE (produce) only the topics the connectors use: cdc.* and mssql.schema-history
#   Schema Registry READ every subject, WRITE (register/evolve) only the cdc.* subjects
#
# Kafbat manages the connectors through the Kafka Connect REST API of the two workers (no Kafka
# permission involved); the connectors keep running with their own service accounts.
# --------------------------------------------------------
resource "confluent_service_account" "kafbat" {
  display_name = "kafbat-${random_id.id.hex}"
  description  = "Kafbat UI: read all topics/subjects, write the connector topics/subjects"
}

locals {
  kafbat_bindings = {
    # key                 = [role, crn pattern]
    read_all_topics      = ["DeveloperRead", "${local.kafka_crn}/topic=*"]
    read_all_groups      = ["DeveloperRead", "${local.kafka_crn}/group=*"]
    write_cdc_topics     = ["DeveloperWrite", "${local.kafka_crn}/topic=${local.topic_prefix}*"]
    write_schema_history = ["DeveloperWrite", "${local.kafka_crn}/topic=mssql.schema-history"]
    read_all_subjects    = ["DeveloperRead", "${local.sr_crn}/subject=*"]
    write_cdc_subjects   = ["DeveloperWrite", "${local.sr_crn}/subject=${local.topic_prefix}*"]
  }
}

resource "confluent_role_binding" "kafbat" {
  for_each    = local.kafbat_bindings
  principal   = "User:${confluent_service_account.kafbat.id}"
  role_name   = each.value[0]
  crn_pattern = each.value[1]
}

resource "confluent_api_key" "kafbat_kafka" {
  display_name = "kafbat-kafka-key-${random_id.id.hex}"
  description  = "Kafka API key of Kafbat UI"
  owner {
    id          = confluent_service_account.kafbat.id
    api_version = confluent_service_account.kafbat.api_version
    kind        = confluent_service_account.kafbat.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cc_kafka_cluster.id
    api_version = confluent_kafka_cluster.cc_kafka_cluster.api_version
    kind        = confluent_kafka_cluster.cc_kafka_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.kafbat]
}

resource "confluent_api_key" "kafbat_sr" {
  display_name = "kafbat-sr-key-${random_id.id.hex}"
  description  = "Schema Registry API key of Kafbat UI"
  owner {
    id          = confluent_service_account.kafbat.id
    api_version = confluent_service_account.kafbat.api_version
    kind        = confluent_service_account.kafbat.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.cc_sr_cluster.id
    api_version = data.confluent_schema_registry_cluster.cc_sr_cluster.api_version
    kind        = data.confluent_schema_registry_cluster.cc_sr_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.kafbat]
}

output "kafbat_credentials" {
  sensitive = true
  value = {
    kafka_key = confluent_api_key.kafbat_kafka.id, kafka_secret = confluent_api_key.kafbat_kafka.secret
    sr_key    = confluent_api_key.kafbat_sr.id, sr_secret = confluent_api_key.kafbat_sr.secret
  }
}
