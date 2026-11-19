#!/bin/bash
# Registers (or removes) the Confluent Cloud managed MCP servers in Claude Code, authenticated with
# the Global API key from .env. Nothing runs locally: the servers are hosted by Confluent.
#
#   ./confluent-mcp.sh            add confluent-mcp-global (+ confluent-mcp-regional for this environment's region)
#   ./confluent-mcp.sh remove     remove both again
#
# Called automatically by start.sh / stop.sh when ENABLE_CONFLUENT_MCP=true (see .env_example).
# CONFLUENT_MCP_SCOPE (default: user) is the Claude Code scope: user = every project, local = this folder only.
set -e
cd "$(dirname "$0")"
source scripts/common.sh   # loads .env
SCOPE=${CONFLUENT_MCP_SCOPE:-user}
NAMES="confluent-mcp-global confluent-mcp-regional"

if ! command -v claude > /dev/null 2>&1; then
  echo "Confluent MCP: the 'claude' CLI is not installed, skipping (see README: Confluent MCP server with Claude)."
  exit 0
fi

if [ "$1" = remove ]; then
  for n in $NAMES; do claude mcp remove -s "$SCOPE" "$n" > /dev/null 2>&1 && echo "Confluent MCP: removed $n" || true; done
  exit 0
fi

if [ -z "$CONFLUENT_GLOBAL_API_KEY" ] || [ -z "$CONFLUENT_GLOBAL_API_SECRET" ]; then
  echo "Confluent MCP: CONFLUENT_GLOBAL_API_KEY / _SECRET are not set in .env, skipping." >&2
  exit 0
fi
[ -f "$ENV_FILE" ] && { set -a; source "$ENV_FILE"; set +a; }
AUTH="Authorization: Basic $(printf '%s:%s' "$CONFLUENT_GLOBAL_API_KEY" "$CONFLUENT_GLOBAL_API_SECRET" | base64 | tr -d '\n')"

add() {  # name url
  claude mcp remove -s "$SCOPE" "$1" > /dev/null 2>&1 || true
  claude mcp add -s "$SCOPE" --transport http "$1" "$2" --header "$AUTH" > /dev/null && echo "Confluent MCP: added $1 ($SCOPE scope)"
}

add confluent-mcp-global https://api.confluent.cloud/mcp/v1
if [ -n "$CC_ORG_ID" ] && [ -n "$CC_SR_REGION" ]; then
  add confluent-mcp-regional "https://mcp.${CC_SR_REGION}.aws.confluent.cloud/mcp/v1/organizations/${CC_ORG_ID}"
else
  echo "Confluent MCP: organization ID / region unknown (run ./start.sh first), regional server not added."
fi
echo "Confluent MCP: restart Claude Code (or run /mcp) to use the servers; the header is stored in Claude's config in clear text."
