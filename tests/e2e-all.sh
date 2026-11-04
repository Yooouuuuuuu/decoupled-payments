#!/usr/bin/env bash
#
# Runs the standard end-to-end matrix with tests/e2e.sh and prints a summary table.
#
# Every implementation mix runs every model. The scenarios with Kafka transactions also run
# with --chaos. Without transactions the system isn't consistent under failures (that is what
# the comparison measures), so that scenario runs without them. A few more scenarios cover
# what this grid doesn't: rejected trades, Java and Go aggregators in one group, Go aggregators
# with Java validators, more banks than validators, and more trades partitions than banks.
# A cluster of one site skips the --chaos scenarios, which need two. A failed scenario doesn't
# stop the others; the exit status is 1 if any scenario failed.
#
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(dirname "$script_dir")

implementations=(
  "--impl java"
  "--impl go"
  "--aggregators java --validators go --tools go"
  "--mixed-validators"
)
models=(
  "--model concurrent --chaos"
  "--model sequential --chaos"
  "--model concurrent --no-tx"
)
more_scenarios=(
  "--mixed-aggregators --mixed-validators --model concurrent --amounts 500-5000 --chaos"
  "--aggregators go --validators java --tools java --model sequential --amounts 500-5000 --zipf --chaos"
  "--impl go --model concurrent --banks 6 --chaos"
  "--impl java --model sequential --trades-partitions 5 --chaos"
)

scenarios=()
for implementation in "${implementations[@]}"; do
  for model in "${models[@]}"; do
    scenarios+=("$implementation $model")
  done
done
scenarios+=("${more_scenarios[@]}")

usage() {
  local scenario
  echo "Usage: tests/e2e-all.sh [e2e.sh options added to every scenario, e.g. --trades 5000]"
  echo
  echo "Runs these ${#scenarios[@]} scenarios with tests/e2e.sh and prints a summary table:"
  for scenario in "${scenarios[@]}"; do
    echo "  $scenario"
  done
}

for argument in "$@"; do
  if [[ $argument == -h || $argument == --help ]]; then
    usage
    exit 0
  fi
done

duration() {  # duration SECONDS, e.g. 2m05s
  printf '%dm%02ds' $(( $1 / 60 )) $(( $1 % 60 ))
}

# The number of sites of the compose file, or nothing if Compose can't read it (then e2e.sh
# reports the problem in every scenario).
compose_sites() {
  local services
  services=$(docker compose --file "$repo_root/infra/docker-compose.yml" --env-file /dev/null \
    config --services 2> /dev/null) || return 0
  grep -c '^validator-' <<< "$services" || true
}

# --chaos needs at least 2 sites, so a smaller cluster skips the scenarios that use it.
needs_two_sites() {  # needs_two_sites OPTION...
  local option
  for option in "$@"; do
    if [[ $option == --chaos ]]; then return 0; fi
  done
  return 1
}

summary_file=$repo_root/results/e2e/$(date -u +%Y%m%dT%H%M%SZ)-matrix.txt
output=$(mktemp)
trap 'rm -f "$output"' EXIT

sites=$(compose_sites)
rows=()
failed=0
skipped=0
started=$SECONDS
for scenario in "${scenarios[@]}"; do
  read -ra options <<< "$scenario"
  options+=("$@")
  if [[ -n $sites ]] && (( sites < 2 )) && needs_two_sites "${options[@]}"; then
    skipped=$(( skipped + 1 ))
    rows+=("$(printf '%-7s %-8s %s' SKIP - "${options[*]}: --chaos needs 2 sites, the cluster has $sites")")
    continue
  fi
  echo
  echo "=== tests/e2e.sh ${options[*]}"
  scenario_started=$SECONDS
  status=0
  # tee ignores Ctrl-C, so that it keeps showing e2e.sh's cleanup until e2e.sh exits.
  "$script_dir/e2e.sh" "${options[@]}" 2>&1 | (trap '' INT; exec tee "$output") || status=$?
  if (( status == 2 || status == 130 || status == 143 )); then
    exit "$status"   # bad options (every scenario would fail the same way), or interrupted
  fi
  if (( status != 0 )); then
    failed=$(( failed + 1 ))
  fi
  # e2e.sh ends with its verdict: "PASS|FAIL <scenario>: <details> (<results>)".
  verdict=$(grep -E '^(PASS|FAIL) ' "$output" | tail -n 1 || true)
  if [[ -z $verdict ]]; then
    verdict="ERROR ${options[*]}: e2e.sh exited with status $status before its verdict"
  fi
  rows+=("$(printf '%-7s %-8s %s' "${verdict%% *}" "$(duration $(( SECONDS - scenario_started )))" "${verdict#* }")")
done

total=${#scenarios[@]}
counts="$(( total - failed - skipped )) passed, $failed failed"
if (( skipped > 0 )); then counts+=", $skipped skipped"; fi
mkdir -p "$(dirname "$summary_file")"
{
  echo
  echo "End-to-end matrix: $total scenarios, $counts, $(duration $(( SECONDS - started ))) in total"
  echo
  printf '%-7s %-8s %s\n' RESULT TIME "SCENARIO: DETAILS (RESULTS)"
  printf '%s\n' "${rows[@]}"
} | tee "$summary_file"
echo
echo "Saved to ${summary_file#"$repo_root"/}"
(( failed == 0 ))
