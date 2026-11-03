# -------------------------------------------------------
# Confluent Cloud Environment (+ Stream Governance / Schema Registry package)
# -------------------------------------------------------
resource "confluent_environment" "cc_demo_env" {
  display_name = "${var.cc_env_name}-${random_id.id.hex}"
  stream_governance {
    package = var.stream_governance
  }
  lifecycle {
    prevent_destroy = false
  }
}
output "cc_demo_env" {
  description = "CC Environment"
  value       = resource.confluent_environment.cc_demo_env.id
}
output "cc_demo_env_name" {
  description = "CC Environment display name"
  value       = resource.confluent_environment.cc_demo_env.display_name
}

# --------------------------------------------------------
# Apache Kafka Cluster (Standard, AWS Frankfurt by default)
# --------------------------------------------------------
resource "confluent_kafka_cluster" "cc_kafka_cluster" {
  display_name = var.cc_cluster_name
  availability = var.cc_availability
  cloud        = var.cc_cloud_provider
  region       = var.cc_cloud_region
  standard {}
  environment {
    id = confluent_environment.cc_demo_env.id
  }
  lifecycle {
    prevent_destroy = false
  }
}
output "cc_kafka_cluster_id" {
  description = "CC Kafka Cluster ID"
  value       = resource.confluent_kafka_cluster.cc_kafka_cluster.id
}
output "cc_kafka_cluster_bootstrap" {
  description = "CC Kafka Cluster Bootstrap Endpoint"
  value       = resource.confluent_kafka_cluster.cc_kafka_cluster.bootstrap_endpoint
}

# --------------------------------------------------------
# Schema Registry: created by Confluent for the environment (Stream Governance package set
# above) once the first Kafka cluster exists. Loaded here to get its endpoint and id.
# --------------------------------------------------------
data "confluent_schema_registry_cluster" "cc_sr_cluster" {
  environment {
    id = resource.confluent_environment.cc_demo_env.id
  }
  depends_on = [
    resource.confluent_kafka_cluster.cc_kafka_cluster
  ]
}
output "cc_sr_cluster_endpoint" {
  value = data.confluent_schema_registry_cluster.cc_sr_cluster.rest_endpoint
}
output "cc_sr_cluster_package" {
  value = data.confluent_schema_registry_cluster.cc_sr_cluster.package
}
output "cc_sr_cluster_region" {
  value = data.confluent_schema_registry_cluster.cc_sr_cluster.region
}

check "schema_registry_package" {
  assert {
    condition     = data.confluent_schema_registry_cluster.cc_sr_cluster.package == var.stream_governance
    error_message = "Schema Registry package is not ${var.stream_governance}"
  }
}

# --------------------------------------------------------
# Service Accounts (app_manager, sr, clients)
# --------------------------------------------------------
resource "confluent_service_account" "app_manager" {
  display_name = "app-manager-${random_id.id.hex}"
  description  = "Application Manager Service Account for Confluent Cloud"
  lifecycle {
    prevent_destroy = false
  }
}
resource "confluent_service_account" "sr" {
  display_name = "sr-${random_id.id.hex}"
  description  = "Schema Registry Service Account for Confluent Cloud"
  lifecycle {
    prevent_destroy = false
  }
}
resource "confluent_service_account" "clients" {
  display_name = "client-${random_id.id.hex}"
  description  = "Kafka Clients (self-managed Connect) Service Account for Confluent Cloud"
  lifecycle {
    prevent_destroy = false
  }
}

# --------------------------------------------------------
# Role Bindings (app_manager, sr, clients)
# --------------------------------------------------------
resource "confluent_role_binding" "app_manager_environment_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.cc_demo_env.resource_name
  lifecycle {
    prevent_destroy = false
  }
}
resource "confluent_role_binding" "sr_environment_admin" {
  principal   = "User:${confluent_service_account.sr.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.cc_demo_env.resource_name
  lifecycle {
    prevent_destroy = false
  }
}
resource "confluent_role_binding" "clients_cluster_admin" {
  principal   = "User:${confluent_service_account.clients.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.cc_kafka_cluster.rbac_crn
  lifecycle {
    prevent_destroy = false
  }
}

# --------------------------------------------------------
# Credentials / API Keys
# --------------------------------------------------------
# app_manager: used by terraform itself to create the topics
resource "confluent_api_key" "app_manager_kafka_cluster_key" {
  display_name = "app-manager-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Application Manager API Key for Confluent Cloud"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
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
    confluent_role_binding.app_manager_environment_admin
  ]
  lifecycle {
    prevent_destroy = false
  }
}

# Schema Registry (used by the Connect converters)
resource "confluent_api_key" "sr_cluster_key" {
  display_name = "sr-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Schema Registry API Key for Confluent Cloud"
  owner {
    id          = confluent_service_account.sr.id
    api_version = confluent_service_account.sr.api_version
    kind        = confluent_service_account.sr.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.cc_sr_cluster.id
    api_version = data.confluent_schema_registry_cluster.cc_sr_cluster.api_version
    kind        = data.confluent_schema_registry_cluster.cc_sr_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [
    confluent_role_binding.sr_environment_admin,
    data.confluent_schema_registry_cluster.cc_sr_cluster
  ]
  lifecycle {
    prevent_destroy = false
  }
}
output "sr_cluster_key" {
  description = "Schema Registry API Key"
  value       = confluent_api_key.sr_cluster_key.id
}
output "sr_cluster_secret" {
  description = "Schema Registry API Key Secret"
  value       = confluent_api_key.sr_cluster_key.secret
  sensitive   = true
}

# Kafka clients (self-managed Kafka Connect worker + connectors)
resource "confluent_api_key" "clients_kafka_cluster_key" {
  display_name = "clients-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Kafka Clients API Key for Confluent Cloud"
  owner {
    id          = confluent_service_account.clients.id
    api_version = confluent_service_account.clients.api_version
    kind        = confluent_service_account.clients.kind
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
    confluent_role_binding.clients_cluster_admin
  ]
  lifecycle {
    prevent_destroy = false
  }
}
output "clients_kafka_cluster_key" {
  description = "Kafka Clients API Key"
  value       = confluent_api_key.clients_kafka_cluster_key.id
}
output "clients_kafka_cluster_secret" {
  description = "Kafka Clients API Key Secret"
  value       = confluent_api_key.clients_kafka_cluster_key.secret
  sensitive   = true
}

# New role bindings / API keys need a moment to propagate before they can be used
resource "time_sleep" "wait_for_rbac" {
  create_duration = "60s"
  depends_on = [
    confluent_api_key.app_manager_kafka_cluster_key,
    confluent_api_key.clients_kafka_cluster_key,
    confluent_role_binding.clients_cluster_admin,
  ]
}

# --------------------------------------------------------
# Kafka topics
# --------------------------------------------------------
# Changing var.run_id replaces this resource, which in turn recreates all topics below
resource "terraform_data" "run" {
  triggers_replace = var.run_id
}

# One compacted topic per replicated table (the Debezium source routes
# mssql.demo_db.dbo.<table> -> <table>). Compaction keeps the latest record of every key.
resource "confluent_kafka_topic" "tables" {
  for_each = local.table_names

  kafka_cluster {
    id = confluent_kafka_cluster.cc_kafka_cluster.id
  }
  topic_name    = each.key
  rest_endpoint = confluent_kafka_cluster.cc_kafka_cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_kafka_cluster_key.id
    secret = confluent_api_key.app_manager_kafka_cluster_key.secret
  }
  partitions_count = 1
  config = {
    "cleanup.policy" = "compact"
  }
  depends_on = [time_sleep.wait_for_rbac]
  lifecycle {
    prevent_destroy      = false
    replace_triggered_by = [terraform_data.run]
  }
}

# Kafka Connect internal topics (self-managed worker) and the Debezium schema history topic
locals {
  connect_topics = {
    "docker-connect-configs" = { "cleanup.policy" = "compact" }
    "docker-connect-offsets" = { "cleanup.policy" = "compact" }
    "docker-connect-status"  = { "cleanup.policy" = "compact" }
    "mssql.schema-history" = {
      "cleanup.policy" = "delete"
      "retention.ms"   = "-1" # the schema history must never expire
    }
  }
}

resource "confluent_kafka_topic" "connect_internal" {
  for_each = local.connect_topics

  kafka_cluster {
    id = confluent_kafka_cluster.cc_kafka_cluster.id
  }
  topic_name    = each.key
  rest_endpoint = confluent_kafka_cluster.cc_kafka_cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_kafka_cluster_key.id
    secret = confluent_api_key.app_manager_kafka_cluster_key.secret
  }
  partitions_count = 1
  config           = each.value
  depends_on       = [time_sleep.wait_for_rbac]
  lifecycle {
    prevent_destroy      = false
    replace_triggered_by = [terraform_data.run]
  }
}

output "cc_topics" {
  description = "Topics created by terraform"
  value       = sort(concat(keys(local.connect_topics), tolist(local.table_names)))
}
