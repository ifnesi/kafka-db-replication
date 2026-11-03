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
    "CC_KAFKA_API_KEY": v("clients_kafka_cluster_key"),
    "CC_KAFKA_API_SECRET": v("clients_kafka_cluster_secret"),
    "CC_SR_URL": v("cc_sr_cluster_endpoint"),
    "CC_SR_API_KEY": v("sr_cluster_key"),
    "CC_SR_API_SECRET": v("sr_cluster_secret"),
    "CC_SR_PACKAGE": v("cc_sr_cluster_package"),
    "CC_SR_REGION": str(v("cc_sr_cluster_region")),
}
for k, val in env.items():
    print(f"{k}='{val}'")
