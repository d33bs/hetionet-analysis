#!/usr/bin/env bash
set -euo pipefail

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
# Tarball URL for Hetionet’s pre-built graph.db (Neo4j 3.0.1 format)
TARBALL_URL="https://github.com/hetio/hetionet/raw/v1.0.0/hetnet/neo4j/hetionet-v1.0.db.tar.bz2"

# Use the matching Neo4j version
NEO4J_IMAGE="neo4j:3.0.12"
APOC_VERSION="3.0.12.0"

# Where to store data, logs, and plugins locally
WORKDIR="${PWD}/hetionet-docker"
HTTP_PORT=7474
BOLT_PORT=7687

# Auth: "none" or "neo4j/YourStrongPw"
NEO4J_AUTH="none"

# Force AMD64 image on ARM hosts
PLATFORM="linux/amd64"

# ─── PREP ───────────────────────────────────────────────────────────────────────
echo "⏳ Creating workspace at $WORKDIR …"
mkdir -p "$WORKDIR/data/databases" "$WORKDIR/logs" "$WORKDIR/plugins"
chmod -R 777 "$WORKDIR/data" "$WORKDIR/logs" "$WORKDIR/plugins"

# ─── DOWNLOAD & EXTRACT ─────────────────────────────────────────────────────────
echo "⏳ Downloading & extracting Hetionet graph.db…"
curl -L "$TARBALL_URL" \
  | tar -xj -C "$WORKDIR/data/databases"

# ─── FETCH APOC PLUGIN ──────────────────────────────────────────────────────────
echo "⏳ Downloading APOC v$APOC_VERSION..."
curl -L \
  -o "$WORKDIR/plugins/apoc-$APOC_VERSION-all.jar" \
  "https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/$APOC_VERSION/apoc-$APOC_VERSION-all.jar"
  
# ─── LAUNCH ─────────────────────────────────────────────────────────────────────
# Remove any old container
if docker ps -a --format '{{.Names}}' | grep -qx hetionet; then
  echo "⏳ Removing old 'hetionet' container…"
  docker rm -f hetionet
fi

echo "🚀 Starting Neo4j 3.0.1 container with APOC…"
docker run -d \
  --platform="$PLATFORM" \
  --name hetionet \
  -p "${HTTP_PORT}:7474" \
  -p "${BOLT_PORT}:7687" \
  -v "${WORKDIR}/data:/data" \
  -v "${WORKDIR}/logs:/logs" \
  -v "${WORKDIR}/plugins:/plugins" \
  -e NEO4J_AUTH="${NEO4J_AUTH}" \
  -e NEO4J_PLUGINS='["apoc"]' \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
  "${NEO4J_IMAGE}"

echo
echo "✅ Done!"
echo "• Browser UI: http://localhost:${HTTP_PORT}"
echo "• Bolt URI:   bolt://localhost:${BOLT_PORT}"
if [[ "${NEO4J_AUTH}" != "none" ]]; then
  echo "• Credentials: neo4j / ${NEO4J_AUTH#*/}"
fi
