# Sourced by start.sh / stop.sh: sets up the container runtime environment.

# `docker compose` plugin if present, else the standalone docker-compose binary
if docker compose version > /dev/null 2>&1; then DC="docker compose"; else DC="docker-compose"; fi

# Colima: talk to its socket directly (works even if the docker context isn't switched)
if command -v colima > /dev/null 2>&1 && [ -S "$HOME/.colima/default/docker.sock" ]; then
  export DOCKER_HOST="${DOCKER_HOST:-unix://$HOME/.colima/default/docker.sock}"
fi

# A ~/.docker/config.json left over from Docker Desktop points at a credential helper that
# is not installed with Colima ("docker-credential-desktop not found"). Use a clean,
# project-local client config in that case (public images only, no login needed).
if grep -q '"credsStore": *"desktop"' "$HOME/.docker/config.json" 2>/dev/null \
   && ! command -v docker-credential-desktop > /dev/null 2>&1; then
  mkdir -p "$ROOT/.docker-config" && echo '{}' > "$ROOT/.docker-config/config.json"
  export DOCKER_CONFIG="$ROOT/.docker-config"
fi

# Colima VM sizing (override with COLIMA_CPU / COLIMA_MEMORY / COLIMA_DISK)
colima_start() {
  command -v colima > /dev/null 2>&1 || return 0
  if colima status > /dev/null 2>&1; then
    echo "Colima already running."
  else
    echo "Starting Colima (vz + Rosetta, ${COLIMA_CPU:-6} CPU / ${COLIMA_MEMORY:-12} GB RAM)..."
    colima start --vm-type vz --vz-rosetta \
      --cpu "${COLIMA_CPU:-6}" --memory "${COLIMA_MEMORY:-12}" --disk "${COLIMA_DISK:-60}"
  fi
  export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
}

colima_stop() {
  command -v colima > /dev/null 2>&1 || return 0
  colima status > /dev/null 2>&1 && { echo "Stopping Colima..."; colima stop; }
}
