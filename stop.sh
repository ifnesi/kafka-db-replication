#!/bin/bash
# Remove the demo stack (and its volumes), then stop Colima.
# Keep the Colima VM running with:  KEEP_COLIMA=1 ./stop.sh
cd "$(dirname "$0")"
ROOT="$PWD"
source scripts/docker-env.sh

if docker info > /dev/null 2>&1; then
  $DC down -v --remove-orphans
fi
[ -n "$KEEP_COLIMA" ] || colima_stop
