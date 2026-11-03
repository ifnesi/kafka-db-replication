locals {
  description = "Resource created using terraform"

  # One topic per replicated table: same single source of truth as the local demo
  # (generated from data-generator/schema_spec.py).
  table_names = toset(compact(split("\n", file("${path.module}/../../scripts/tables.txt"))))
}

# --------------------------------------------------------
# This 'random_id' makes whatever you create (names, etc)
# unique in your account: db-replication-xxxxxx
# --------------------------------------------------------
resource "random_id" "id" {
  byte_length = 3 # 6 hex characters
}

# ----------------------------------------
# Confluent Cloud Kafka cluster variables
# ----------------------------------------
variable "cc_cloud_provider" {
  type    = string
  default = "AWS"
}

variable "cc_cloud_region" {
  type    = string
  default = "eu-central-1" # AWS Frankfurt
}

variable "cc_env_name" {
  type    = string
  default = "db-replication"
}

variable "cc_cluster_name" {
  type    = string
  default = "cc-demo-main"
}

variable "cc_availability" {
  type    = string
  default = "SINGLE_ZONE"
}

variable "stream_governance" {
  type    = string
  default = "ESSENTIALS" # Schema Registry package
}

# Changing this value (start.sh passes a new one on every start) recreates the topics, so
# every start begins with empty topics, matching the freshly created source/target databases.
variable "run_id" {
  type    = string
  default = "initial"
}
