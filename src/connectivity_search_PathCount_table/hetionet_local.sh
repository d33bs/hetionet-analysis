#!/usr/bin/env bash
set -euo pipefail

WORKDIR="${PWD}/hetionet-upgrade"
PLATFORM="linux/amd64"
IMG32="neo4j:3.2"

# 1) Make sure you’ve extracted your 3.0.1 graph.db into $WORKDIR/data/databases/neo4j
#    and shut down any running container using it.

# 2) (Optional) Backup your raw store directory:
cp -r "$WORKDIR/data/databases/neo4j" "$WORKDIR/data/databases/neo4j-backup-3.0.1"

# 3) Run Neo4j 3.2 with format‐migration enabled.
echo "🚚 Starting Neo4j 3.2 to migrate v0.A.7 → v0.A.8…"
docker rm -f migrate32 2>/dev/null || true
docker run -d --name migrate32 --platform="$PLATFORM" \
  -v "$WORKDIR/data:/data" \
  -v "$WORKDIR/logs:/logs" \
  -e NEO4J_AUTH="none" \
  -e NEO4J_dbms_allow_format_migration=true \
  "$IMG32"

# 4) Wait for the upgrade to finish.
#    Neo4j logs progress to debug.log; when you see “Completed store upgrade” you’re done.
echo "⏳ Waiting for format migration to finish (watch debug.log)..."
until docker logs migrate32 2>&1 | grep -q "Completed store upgrade"; do
  sleep 1
done

# 5) Shut down the 3.2 container gracefully so it writes the new on-disk format.
echo "🛑 Stopping Neo4j 3.2…"
docker stop migrate32

echo "✔ In-place upgrade complete. Your store is now in v0.A.8 format."


# echo "⏳ Waiting for 3.2 to finish its upgrade (look for Bolt)…"
# until docker logs migrate32 2>&1 | grep -q "Bolt enabled on"; do sleep 1; done

# echo "🛑 Stopping Neo4j 3.2 gracefully…"
# docker stop migrate32
# echo "✔ Store is now at v0.A.8 (3.2 format)."

# # ─── VERIFY STORE VERSION ───────────────────────────────────────────────────────
# echo "🔍 Verifying store version:"
# docker run --rm --platform="$PLATFORM" \
#   -v "$WORKDIR/data:/data" \
#   "$IMG32" \
#   bash -c "neo4j-admin store-info --store=/data/databases/neo4j"

# # ─── STEP 3: DUMP THE UPGRADED STORE WITH 3.5 ───────────────────────────────────
# echo "🚀 Dumping the v0.A.8 store to step1.dump…"
# docker run --rm --platform="$PLATFORM" \
#   -v "$WORKDIR/data:/data" \
#   -v "$WORKDIR/import:/import" \
#   -e NEO4J_AUTH="$NEO4J_AUTH" \
#   "$IMG35" \
#   neo4j-admin dump --database=neo4j --to=/import/step1.dump
# echo "✔ step1.dump ready."

# # ─── STEP 4: LOAD & RE-DUMP WITH 4.4 ─────────────────────────────────────────────
# echo "🚀 Loading step1.dump into 4.4 and dumping to step2.dump…"
# docker run --rm --platform="$PLATFORM" \
#   -v "$WORKDIR/data:/data" \
#   -v "$WORKDIR/import:/import" \
#   -e NEO4J_AUTH="$NEO4J_AUTH" \
#   "$IMG44" \
#   bash -c "\
#     neo4j-admin load --database=neo4j --from=/import/step1.dump --force && \
#     neo4j-admin dump --database=neo4j --to=/import/step2.dump"
# echo "✔ step2.dump ready."

# # ─── STEP 5: LOAD INTO NEO4J 5 & START ───────────────────────────────────────────
# echo "🚀 Loading step2.dump into Neo4j 5.8 and launching server…"
# docker run --rm --platform="$PLATFORM" \
#   -v "$WORKDIR/data:/data" \
#   -v "$WORKDIR/import:/import" \
#   -e NEO4J_AUTH="$NEO4J_AUTH" \
#   "$IMG5" \
#   neo4j-admin database load neo4j \
#     --from-path=/import/step2.dump \
#     --overwrite-destination=true

# docker rm -f he5 2>/dev/null || true
# docker run -d --platform="$PLATFORM" --name he5 \
#   -p "${HTTP_PORT}:7474" \
#   -p "${BOLT_PORT}:7687" \
#   -v "$WORKDIR/data:/data" \
#   -v "$WORKDIR/logs:/logs" \
#   -e NEO4J_AUTH="$NEO4J_AUTH" \
#   -e NEO4J_initial_dbms_default__database=neo4j \
#   "$IMG5"

# echo
# echo "✅ All done! Browse to http://localhost:${HTTP_PORT} to see your Hetionet graph."
