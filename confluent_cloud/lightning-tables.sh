#!/bin/bash
# Enables Lightning Tables on a few topics of the demo cluster and prepares everything a local
# DBeaver needs to query them over JDBC: the driver JAR and a connection properties file.
#
#   ./lightning-tables.sh                 enable the default topics, download driver, write properties
#   TOPICS="orders customers" ./lightning-tables.sh
#   ./lightning-tables.sh disable         disable Lightning Tables on those topics again
#
# Env vars (prompted for when missing):
# Credentials come from .env (copy .env_example) or the environment:
#   CONFLUENT_CLOUD_API_KEY / _SECRET     Cloud resource management key (enable/disable via REST)
#   CONFLUENT_GLOBAL_API_KEY / _SECRET    Global API key used by the JDBC driver (and the MCP server)
set -e
cd "$(dirname "$0")"
[ -f cloud.env ] || { echo "ERROR: cloud.env not found, run ./start.sh first." >&2; exit 1; }
source cloud.env
source scripts/common.sh   # loads .env

TOPICS=${TOPICS:-"data_sensors customers orders products"}
CLOUD=AWS
REGION=${CC_SR_REGION:-eu-central-1}
DRIVER_VERSION=1.0.0
DRIVER=drivers/confluent-jdbc-$DRIVER_VERSION.jar
PROPS=lightning-jdbc.properties

if [ -z "$CONFLUENT_CLOUD_API_KEY" ]; then read -rp "Confluent Cloud API key: " CONFLUENT_CLOUD_API_KEY; fi
if [ -z "$CONFLUENT_CLOUD_API_SECRET" ]; then read -rsp "Confluent Cloud API secret: " CONFLUENT_CLOUD_API_SECRET; echo; fi

api() { # method topic [body]
  curl -s -o /tmp/lt.out -w '%{http_code}' -X "$1" -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
    -H 'Content-Type: application/json' ${3:+-d "$3"} "$2"
}

if [ "$1" = disable ]; then
  for t in $TOPICS; do
    code=$(api DELETE "https://api.confluent.cloud/rtce/v1/rtce-topics/$t?environment=$CC_ENV_ID&spec.kafka_cluster=$CC_CLUSTER_ID")
    echo "disable $t -> HTTP $code"
  done
  exit 0
fi

for t in $TOPICS; do
  body=$(printf '{"spec":{"cloud":"%s","region":"%s","topic_name":"%s","environment":{"id":"%s"},"kafka_cluster":{"id":"%s"}}}' \
    "$CLOUD" "$REGION" "$t" "$CC_ENV_ID" "$CC_CLUSTER_ID")
  code=$(api POST https://api.confluent.cloud/rtce/v1/rtce-topics "$body")
  echo "enable  $t -> HTTP $code"
  case $code in 2*|409) ;; *) cat /tmp/lt.out; echo; exit 1;; esac
done
rm -f /tmp/lt.out

mkdir -p drivers
[ -f "$DRIVER" ] || curl -fL -o "$DRIVER" "https://packages.confluent.io/maven/io/confluent/jdbc/confluent-jdbc/$DRIVER_VERSION/confluent-jdbc-$DRIVER_VERSION.jar"

if [ -z "$CONFLUENT_GLOBAL_API_KEY" ]; then read -rp "Confluent Global API key: " CONFLUENT_GLOBAL_API_KEY; fi
if [ -z "$CONFLUENT_GLOBAL_API_SECRET" ]; then read -rsp "Confluent Global API secret: " CONFLUENT_GLOBAL_API_SECRET; echo; fi
GKEY=$CONFLUENT_GLOBAL_API_KEY
GSECRET=$CONFLUENT_GLOBAL_API_SECRET
( umask 077; cat > "$PROPS" <<PROPS
client.lightning-endpoint=sql.$REGION.aws.confluent.cloud
client.environment-id=$CC_ENV_ID
client.lkc-id=$CC_CLUSTER_ID
client.global-api-key=$GKEY
client.global-api-secret=$GSECRET
PROPS
)

cat <<EOT

Lightning Tables are being enabled (takes several minutes; status in the console: Topics > Lightning Tables).

DBeaver: Database > Driver Manager > New
  Driver class : io.confluent.jdbc.avatica.ConfluentJdbcDriver
  Library      : $PWD/$DRIVER
  URL template : jdbc:confluent:lightning://?propertiesFile=$PWD/$PROPS
Then New connection with that driver (leave user/password empty) and run, e.g.:
  SELECT * FROM data_sensors LIMIT 10;
EOT
