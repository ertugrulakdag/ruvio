#!/usr/bin/env bash
# Build a Maven Central bundle from the signed Java client artifacts and upload it to the Central Portal.
#
#   scripts/central_upload.sh <user-managed|automatic>
#
# Run `mvn -P release verify -DskipTests` in clients/java first (it builds and signs the jars).
# Needs CENTRAL_USERNAME and CENTRAL_PASSWORD (Portal user token) unless CENTRAL_DRY_RUN=1.
set -euo pipefail

mode="${1:-user-managed}"
case "$mode" in
  user-managed) publishing_type=USER_MANAGED ;;
  automatic) publishing_type=AUTOMATIC ;;
  *) echo "mode must be user-managed or automatic" >&2; exit 2 ;;
esac

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
java="$root/clients/java"
group_path=io/github/salihcantekin
artifact=ruvio-client
version=$(sed -n 's#^  <version>\(.*\)</version>#\1#p' "$java/pom.xml" | head -1)
[ -n "$version" ] || { echo "no version in clients/java/pom.xml" >&2; exit 1; }

work="$java/target/central-bundle"
dir="$work/$group_path/$artifact/$version"
rm -rf "$work"
mkdir -p "$dir"

base="$artifact-$version"
cp "$java/target/$base.jar" "$java/target/$base-sources.jar" "$java/target/$base-javadoc.jar" "$dir/"
cp "$java/pom.xml" "$dir/$base.pom"
for f in "$base.jar" "$base-sources.jar" "$base-javadoc.jar"; do
  cp "$java/target/$f.asc" "$dir/$f.asc"
done
cp "$java/target/$base.pom.asc" "$dir/$base.pom.asc"

for f in "$base.jar" "$base-sources.jar" "$base-javadoc.jar" "$base.pom"; do
  (cd "$dir" && md5sum "$f" | cut -d' ' -f1 | tr -d '\n' > "$f.md5"
   sha1sum "$f" | cut -d' ' -f1 | tr -d '\n' > "$f.sha1"
   sha256sum "$f" | cut -d' ' -f1 | tr -d '\n' > "$f.sha256"
   sha512sum "$f" | cut -d' ' -f1 | tr -d '\n' > "$f.sha512")
done

bundle="$java/target/central-bundle.zip"
rm -f "$bundle"
(cd "$work" && zip -q -r -D "$bundle" io)
echo "bundle entries:"
unzip -Z1 "$bundle" | sort

if [ "${CENTRAL_DRY_RUN:-0}" = 1 ]; then
  echo "dry run: not uploading"
  exit 0
fi

: "${CENTRAL_USERNAME:?}" "${CENTRAL_PASSWORD:?}"
auth=$(printf '%s:%s' "$CENTRAL_USERNAME" "$CENTRAL_PASSWORD" | base64 | tr -d '\n')
api=https://central.sonatype.com/api/v1/publisher

id=$(curl -sS --fail-with-body -X POST -H "Authorization: Bearer $auth" \
  -F "bundle=@$bundle" "$api/upload?name=$artifact-$version&publishingType=$publishing_type")
echo "uploaded deployment: $id"

for _ in $(seq 1 60); do
  status=$(curl -sS --fail-with-body -X POST -H "Authorization: Bearer $auth" "$api/status?id=$id")
  state=$(printf '%s' "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("deploymentState",""))')
  echo "state: $state"
  case "$state" in
    VALIDATED|PUBLISHING|PUBLISHED) printf '%s\n' "$status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("errors:", d.get("errors", {}))'; exit 0 ;;
    FAILED) printf '%s\n' "$status"; exit 1 ;;
  esac
  sleep 5
done
echo "timed out waiting for validation" >&2
exit 1
