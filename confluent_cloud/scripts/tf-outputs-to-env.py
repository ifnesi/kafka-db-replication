#!/usr/bin/env python3
"""Reads `terraform output -json` on stdin and writes the docker compose env file (stdout).

Contains secrets (API keys): the file is git-ignored and written with mode 600 by start.sh.
"""
import json
import sys

o = json.load(sys.stdin)


def v(key):
    return o[key]["value"]


env = {
    "CC_ENV_ID": v("cc_demo_env"),
    "CC_ENV_NAME": v("cc_demo_env_name"),
    "CC_CLUSTER_ID": v("cc_kafka_cluster_id"),
    # SASL_SSL://pkc-xxxx.eu-central-1.aws.confluent.cloud:9092 -> host:port
    "CC_KAFKA_BOOTSTRAP": v("cc_kafka_cluster_bootstrap").split("://", 1)[-1],
    "CC_SR_URL": v("cc_sr_cluster_endpoint"),
    "CC_SR_PACKAGE": v("cc_sr_cluster_package"),
    "CC_SR_REGION": str(v("cc_sr_cluster_region")),
}
# one Kafka (+ Schema Registry) API key per identity: worker, source, pgsink, db2sink
for ident, c in v("connect_credentials").items():
    prefix = f"CC_{ident.upper()}_"
    env[prefix + "KAFKA_API_KEY"] = c["kafka_key"]
    env[prefix + "KAFKA_API_SECRET"] = c["kafka_secret"]
    if "sr_key" in c:
        env[prefix + "SR_API_KEY"] = c["sr_key"]
        env[prefix + "SR_API_SECRET"] = c["sr_secret"]
for k, val in env.items():
    print(f"{k}='{val}'")
