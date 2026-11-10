#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?set KUBECONFIG to the test cluster kubeconfig}"
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
KINFCTL=${KINFCTL:-"$ROOT/target/debug/kinfctl"}
EVIDENCE_DIR=${EVIDENCE_DIR:-$(mktemp -d /tmp/kinference-policy-admission.XXXXXX)}
mkdir -p "$EVIDENCE_DIR"
count=0
fixtures="$ROOT/crates/kinference-policy/src/check/admission-fixtures.json"
expected_count=$(jq 'length' "$fixtures")
while IFS= read -r fixture; do
    name=$(jq -r '.name' <<< "$fixture")
    expected=$(jq -r '.accepted' <<< "$fixture")
    manifest=$(jq '{apiVersion:"kinference.buoyant.io/v1alpha1",kind:"InferencePolicy",metadata:{name:.name},spec:.spec}' <<< "$fixture")
    if printf '%s' "$manifest" | kubectl create --dry-run=server --validate=false -f - > "$EVIDENCE_DIR/$name.server.log" 2>&1; then
        server=true
    else
        if ! grep -q 'is invalid:' "$EVIDENCE_DIR/$name.server.log"; then
            cat "$EVIDENCE_DIR/$name.server.log" >&2
            exit 1
        fi
        server=false
    fi
    if printf '%s' "$manifest" | "$KINFCTL" check -o json - > "$EVIDENCE_DIR/$name.cli.json"; then
        cli=true
    else
        status=$?
        if [[ "$status" != 1 ]]; then exit "$status"; fi
        cli=false
    fi
    printf '%s expected=%s server=%s cli=%s\n' "$name" "$expected" "$server" "$cli"
    [[ "$server" == "$expected" && "$cli" == "$expected" ]]
    count=$((count + 1))
done < <(jq -c '.[]' "$fixtures")
[[ "$count" -gt 0 && "$count" == "$expected_count" ]]
printf 'PASS policy-admission fixtures=%s\n' "$count"
