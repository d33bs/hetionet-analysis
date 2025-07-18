#!/usr/bin/env bash
set -euo pipefail

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
WORKDIR="$PWD/hetionet-docker"
DATA_30="$WORKDIR/data/databases/graph.db"
DATA_35="$WORKDIR/data35/databases/graph.db"
DATA_40="$WORKDIR/data40"
BACKUP_DIR="$WORKDIR/backup"

HTTP_35=7474; BOLT_35=7687
HTTP_40=7474; BOLT_40=7687

PLATFORM="linux/amd64"
V35="3.5.35"
V40="4.4.4"
APOC_VERSION="4.4.4.0"

# ─── PREP ───────────────────────────────────────────────────────────────────────
mkdir -p "$WORKDIR/data35" "$WORKDIR/data40" "$BACKUP_DIR"
mkdir -p "$WORKDIR/plugins"

# ─── STEP 1: COPY 3.0.12 STORE → 3.5 FOLDER ───────────────────────────────────────
echo "⏳ Copying 3.0.12 graph.db → 3.5 data folder"
rm -rf "$WORKDIR/data35"
mkdir -p "$(dirname "$DATA_35")"
cp -a "$DATA_30" "$(dirname "$DATA_35")"

# Remove legacy .id files that prevent startup
echo "🧹 Cleaning up old ID files before upgrade"
find "$(dirname "$DATA_35")/graph.db" -type f -name '*.id' -delete
rm "$WORKDIR/backup/graph-3.5.dump" 2>/dev/null || true

# ─── STEP 2: START 3.5 TO UPGRADE STORE ─────────────────────────────────────────
echo "🚀 Starting Neo4j 3.5 to auto-upgrade store format…"
docker run -d --name neo4j35-upgrade \
  --platform="$PLATFORM" \
  -p "$HTTP_35":7474 -p "$BOLT_35":7687 \
  -v "$WORKDIR/data35":/data \
  -e NEO4J_AUTH="none" \
  -e NEO4J_dbms_allow__upgrade="true" \
  neo4j:$V35

# Wait for migration completion message
echo -n "⏳ Waiting for migration completion logs"
until docker logs neo4j35-upgrade 2>&1 | grep -q "Successfully finished upgrade of database"; do
  echo -n .; sleep 1
done
echo " ✔ Store successfully upgraded by 3.5"

# Stop 3.5 container
echo "⏳ Stopping 3.5 container"
docker rm -f neo4j35-upgrade

# ─── STEP 3: DUMP 3.5 STORE TO .dump ─────────────────────────────────────────────
echo "⏳ Dumping upgraded 3.5 store → $BACKUP_DIR/graph-3.5.dump"
docker run --rm \
  --platform="$PLATFORM" \
  -v "$WORKDIR/data35":/data \
  -v "$BACKUP_DIR":/backup \
  neo4j:$V35 \
  /var/lib/neo4j/bin/neo4j-admin dump \
    --database=graph.db \
    --to=/backup/graph-3.5.dump

echo "✅ 3.5 → dump complete"

# ─── STEP 4: LOAD INTO 4.0 ───────────────────────────────────────────────────────
echo "⏳ Loading dump into Neo4j 4.0 → data40/databases/neo4j"
docker run --rm \
  --platform="$PLATFORM" \
  -v "$BACKUP_DIR":/backup \
  -v "$WORKDIR/data40":/data \
  -v "$WORKDIR/data35":/data35 \
  -e NEO4J_dbms_allow__upgrade="true" \
  neo4j:$V40 \
  /var/lib/neo4j/bin/neo4j-admin load \
    --database=neo4j \
    --from=/backup/graph-3.5.dump --force

echo "✅ 4.0 load complete"

rm -rf "$WORKDIR/data40/transactions/neo4j/"*

echo "⏳ Starting 4.4 for clean shutdown..."
docker run -d --name neo4j40_migrate \
  --platform="$PLATFORM" \
  -v "$WORKDIR/data40":/data \
  -e NEO4J_AUTH="neo4j/hetionet" \
  -e NEO4J_dbms_allow__upgrade="true" \
  neo4j:$V40
# Wait for startup
until docker logs neo4j40_migrate 2>&1 | grep -q "Started\."; do
  sleep 1
done
echo " ✔ 4.4 started"

echo "⏳ Stopping 4.4 to complete migration and clear logs"
docker stop neo4j40_migrate >/dev/null
docker rm neo4j40_migrate >/dev/null

docker run --rm \
  --platform="$PLATFORM" \
  -v "$WORKDIR/data40":/data \
  -e NEO4J_dbms_allow__upgrade="true" \
  neo4j:$V40 \
  /var/lib/neo4j/bin/neo4j-admin check-consistency --database=neo4j

echo "✅ 4.0 recovery complete"

# ─── STEP 5: START & VERIFY 4.0 ─────────────────────────────────────────────────
echo "🚀 Starting Neo4j 4.0 container…"
docker run -d --name neo4j40 \
  --platform="$PLATFORM" \
  -p "$HTTP_40":7474 \
  -p "$BOLT_40":7687 \
  -v "$WORKDIR/data40":/data \
  -v "${WORKDIR}/plugins:/plugins" \
  -v /tmp:/tmp \
  -e NEO4J_AUTH="none" \
  -e NEO4JLABS_PLUGINS='["apoc"]' \
  -e NEO4J_apoc_import_file_use__neo4j__config=false \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
  -e NEO4J_AUTH="neo4j/hetionet" \
  -e NEO4J_dbms_allow__upgrade="true" \
  neo4j:$V40

echo
 echo "🎉 Upgrade to 4.0 complete!"
echo "• HTTP: http://localhost:${HTTP_40}"
echo "• Bolt: bolt://localhost:${BOLT_40}"
