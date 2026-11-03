# Sourced by start.sh / stop.sh / deploy-connectors.sh / verify.sh

CC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$CC_DIR/.." && pwd)"
ROOT="$REPO_ROOT"                      # used by docker-env.sh for the project-local docker config
ENV_FILE="$CC_DIR/cloud.env"           # generated from terraform outputs (secrets, git-ignored)
TF_DIR="$CC_DIR/terraform"

# DC (docker compose / docker-compose), DOCKER_HOST for Colima, credential-helper workaround,
# colima_start / colima_stop: shared with the local-only demo
source "$REPO_ROOT/scripts/docker-env.sh"

# docker compose wrapper: passes cloud.env (if it exists) so ${CC_...} variables resolve
dc() {
  if [ -f "$ENV_FILE" ]; then $DC --env-file "$ENV_FILE" "$@"; else $DC "$@"; fi
}

# ---------------------------------------------------------------------------
# Confluent Cloud credentials: taken from the environment or prompted (never stored on disk)
# ---------------------------------------------------------------------------
require_cc_credentials() {
  if [ -z "$CONFLUENT_CLOUD_API_KEY" ] || [ -z "$CONFLUENT_CLOUD_API_SECRET" ]; then
    if [ ! -t 0 ]; then
      echo "ERROR: CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET are not set and there is no terminal to ask for them." >&2
      exit 1
    fi
    echo "Terraform needs a Confluent Cloud API key (Cloud resource management key, see README.md)."
    [ -n "$CONFLUENT_CLOUD_API_KEY" ] || read -r -p "  CONFLUENT_CLOUD_API_KEY:    " CONFLUENT_CLOUD_API_KEY
    [ -n "$CONFLUENT_CLOUD_API_SECRET" ] || { read -r -s -p "  CONFLUENT_CLOUD_API_SECRET: " CONFLUENT_CLOUD_API_SECRET; echo; }
  fi
  if [ -z "$CONFLUENT_CLOUD_API_KEY" ] || [ -z "$CONFLUENT_CLOUD_API_SECRET" ]; then
    echo "ERROR: API key and secret are required." >&2
    exit 1
  fi
  export CONFLUENT_CLOUD_API_KEY CONFLUENT_CLOUD_API_SECRET
}

# ---------------------------------------------------------------------------
# Terraform: local binary if installed, otherwise the official hashicorp/terraform image
# (force the image with TF_USE_DOCKER=1). State lives in confluent_cloud/terraform/.
# ---------------------------------------------------------------------------
tf() {
  if [ -z "$TF_USE_DOCKER" ] && command -v terraform > /dev/null 2>&1; then
    (cd "$TF_DIR" && terraform "$@")
  else
    local args=(-e CONFLUENT_CLOUD_API_KEY -e CONFLUENT_CLOUD_API_SECRET -e TF_IN_AUTOMATION=1)
    local v
    for v in $(compgen -e | grep '^TF_VAR_' || true); do args+=(-e "$v"); done
    docker run --rm "${args[@]}" -v "$REPO_ROOT":/repo -w /repo/confluent_cloud/terraform \
      "hashicorp/terraform:${TERRAFORM_IMAGE_TAG:-1.17}" "$@"
  fi
}

# true if terraform state holds resources (i.e. a cloud environment is deployed)
tf_has_state() {
  [ -s "$TF_DIR/terraform.tfstate" ] && python3 - "$TF_DIR/terraform.tfstate" <<'PY'
import json, sys
sys.exit(0 if json.load(open(sys.argv[1])).get("resources") else 1)
PY
}
