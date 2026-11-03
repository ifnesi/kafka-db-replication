#!/bin/bash
# Remove the local stack (and its volumes), optionally destroy the Confluent Cloud
# resources created by terraform, then stop Colima.
#
#   ./stop.sh                  asks whether to destroy the cloud environment too
#   DESTROY_CLOUD=1 ./stop.sh  destroy it without asking (needs the API key/secret)
#   DESTROY_CLOUD=0 ./stop.sh  keep it, don't ask
#   KEEP_COLIMA=1 ./stop.sh    keep the Colima VM running
cd "$(dirname "$0")"
source scripts/common.sh

if docker info > /dev/null 2>&1; then
  dc down -v --remove-orphans

  if tf_has_state; then
    destroy="$DESTROY_CLOUD"
    if [ -z "$destroy" ] && [ -t 0 ]; then
      read -r -p "Also destroy the Confluent Cloud environment, cluster and topics? (billed while they exist) [y/N] " ans
      case "$ans" in [yY]*) destroy=1 ;; *) destroy=0 ;; esac
    fi
    if [ "$destroy" = "1" ]; then
      require_cc_credentials
      tf destroy -input=false -auto-approve -var "run_id=destroy" && rm -f "$ENV_FILE"
    else
      echo "Confluent Cloud resources kept (run ./stop.sh again and answer 'y', or DESTROY_CLOUD=1 ./stop.sh, to remove them)."
    fi
  fi
fi

[ -n "$KEEP_COLIMA" ] || colima_stop
