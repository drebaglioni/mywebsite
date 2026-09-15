#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
checkpoint_dir=${FLYBODY_WALKING_POLICY:-"$repo_root/.context/flybody-data/trained-fly-policies/walking"}
output_dir=${FLYBODY_BRIDGE_OUTPUT:-"$repo_root/.context/flybody-output"}

if [ ! -f "$checkpoint_dir/saved_model.pb" ]; then
    echo "Walking checkpoint not found: $checkpoint_dir" >&2
    echo "Download Figshare file 44815195 and extract its walking/ directory there." >&2
    exit 2
fi

mkdir -p "$output_dir"

docker build \
    --progress plain \
    --tag fly-gambit-flybody:local \
    --file "$repo_root/experiments/flybody-bridge/Dockerfile" \
    "$repo_root"

docker run --rm \
    --volume "$repo_root:/workspace" \
    --volume "$checkpoint_dir:/models/walking:ro" \
    --volume "$output_dir:/output" \
    fly-gambit-flybody:local \
    --policy /workspace/data/fly-gambit-policy.json \
    --walking-policy /models/walking \
    --output /output
