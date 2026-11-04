#!/usr/bin/env bash
#
# End-to-end test of the local cluster in infra/docker-compose.yml.
#
# Starts the cluster (Kafka, init, aggregators, validators), sends trades with the trade
# generator and runs the report. The report reads everything back from Kafka and checks that
# every trade was validated exactly once, every accepted trade was completed exactly once, and
# no money was created or lost. With --chaos, processes are killed, restarted and paused while
# the trades flow, which exercises recovery from Kafka and the fencing of zombie aggregators
# and validators. After the report, every aggregator and validator must be running and in its
# consumer group again.
#
# Every container's log and the report are saved to results/e2e/<UTC timestamp>-<scenario>/.
# The last line printed is the verdict: PASS or FAIL, the scenario and the key numbers.
# Exit status: 0 if the test passed, 1 otherwise, 2 for bad options.
#
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(dirname "$script_dir")
compose_file=$repo_root/infra/docker-compose.yml

readonly REPORT_TIMEOUT_MS=180000   # how long the report waits for trades still in flight
readonly STARTUP_TIMEOUT_S=180      # how long the processes may take to join their groups
readonly RECOVERY_TIMEOUT_S=60      # how long they may take to be back in their groups after the report

# The chaos, in seconds. It starts while the generator sends trades.
readonly CHAOS_START_S=3            # when it SIGKILLs two processes and pauses two others
readonly KILLED_FOR_S=5             # how long the SIGKILLed processes stay down
readonly PAUSED_FOR_S=25            # how long the paused ones sleep: well over the 10 s session timeout
killed=(validator-1 aggregator-1)   # SIGKILLed, then restarted
paused=(validator-2 aggregator-2)   # paused, so that they wake up as zombies

usage() {
  cat <<'EOF'
Usage: tests/e2e.sh [options]

Runs the local cluster (infra/docker-compose.yml) end to end and checks the outcome with the
report. Exits with 0 if the test passed.

Implementations (java or go; java by default):
  --impl IMPL           every role
  --aggregators IMPL    the aggregators
  --validators IMPL     the validators
  --tools IMPL          init, the trade generator and the report
  --mixed-aggregators   the aggregators of odd sites in java, of even sites in go
  --mixed-validators    the validators of odd sites in java, of even sites in go

Scenario:
  --model MODEL         concurrent (default) or sequential
  --no-tx               run without Kafka transactions (not consistent under failures)
  --trades N            trades to generate (default 20000)
  --rps R               trades per second (default 2000)
  --amounts MIN-MAX     trade amounts (default 1-100). Amounts like 500-5000 make balances
                        run short, so that trades are rejected: with this option the test
                        also requires at least one rejected trade
  --zipf                pick the accounts of a trade with a Zipf distribution, not uniformly
  --banks N             number of banks (default: one per site); with more banks than
                        sites, validators own several
  --trades-partitions N partitions of the trades topic (default: one per site)
  --chaos               while the trades flow: SIGKILL validator-1 and aggregator-1 and
                        restart them 5 s later; pause validator-2 and aggregator-2 for 25 s,
                        well over the 10 s session timeout, so that their partitions move to
                        the other processes before they wake up as zombies

Other:
  --keep                leave the cluster running at the end
  -h, --help            show this help

Every container's log, the generator's and the report's output are saved to
results/e2e/<UTC timestamp>-<scenario>/.
EOF
}

fail_usage() {
  echo "e2e.sh: $*" >&2
  echo "Run 'tests/e2e.sh --help' for usage." >&2
  exit 2
}

# --- Options ---------------------------------------------------------------------------------

impl=java aggregators="" validators="" tools="" mixed_aggregators=false mixed_validators=false
model=concurrent transactions=true trades=20000 rps=2000 amounts="" zipf=false
banks="" trades_partitions="" chaos=false keep=false

need_value() {  # need_value OPTION [VALUE...]
  (( $# >= 2 )) || fail_usage "$1 needs a value"
}

while (( $# > 0 )); do
  case $1 in
    # --name=value is the same as --name value.
    --impl=* | --aggregators=* | --validators=* | --tools=* | --model=* | --trades=* | --rps=* | \
    --amounts=* | --banks=* | --trades-partitions=*)
      set -- "${1%%=*}" "${1#*=}" "${@:2}"
      continue ;;
    --impl) need_value "$@"; impl=$2; shift ;;
    --aggregators) need_value "$@"; aggregators=$2; shift ;;
    --validators) need_value "$@"; validators=$2; shift ;;
    --tools) need_value "$@"; tools=$2; shift ;;
    --mixed-aggregators) mixed_aggregators=true ;;
    --mixed-validators) mixed_validators=true ;;
    --model) need_value "$@"; model=$2; shift ;;
    --no-tx) transactions=false ;;
    --trades) need_value "$@"; trades=$2; shift ;;
    --rps) need_value "$@"; rps=$2; shift ;;
    --amounts) need_value "$@"; amounts=$2; shift ;;
    --zipf) zipf=true ;;
    --banks) need_value "$@"; banks=$2; shift ;;
    --trades-partitions) need_value "$@"; trades_partitions=$2; shift ;;
    --chaos) chaos=true ;;
    --keep) keep=true ;;
    -h | --help) usage; exit 0 ;;
    *) fail_usage "unknown option: $1" ;;
  esac
  shift
done

is_impl() {
  [[ $1 == java || $1 == go ]]
}

is_count() {  # a positive integer
  [[ $1 =~ ^[1-9][0-9]*$ ]]
}

is_impl "$impl" || fail_usage "--impl must be java or go, got '$impl'"
[[ -z $aggregators ]] || is_impl "$aggregators" || fail_usage "--aggregators must be java or go, got '$aggregators'"
[[ -z $validators ]] || is_impl "$validators" || fail_usage "--validators must be java or go, got '$validators'"
[[ -z $tools ]] || is_impl "$tools" || fail_usage "--tools must be java or go, got '$tools'"
if [[ $mixed_aggregators == true && -n $aggregators ]]; then
  fail_usage "--aggregators and --mixed-aggregators exclude each other"
fi
if [[ $mixed_validators == true && -n $validators ]]; then
  fail_usage "--validators and --mixed-validators exclude each other"
fi
[[ $model == concurrent || $model == sequential ]] || fail_usage "--model must be concurrent or sequential, got '$model'"
is_count "$trades" || fail_usage "--trades must be a positive integer, got '$trades'"
is_count "$rps" || fail_usage "--rps must be a positive integer, got '$rps'"
[[ -z $banks ]] || is_count "$banks" || fail_usage "--banks must be a positive integer, got '$banks'"
[[ -z $trades_partitions ]] || is_count "$trades_partitions" ||
  fail_usage "--trades-partitions must be a positive integer, got '$trades_partitions'"

# The trade generator's settings. At most 18 digits per amount, so that bash can compare them.
generator_options=("--generator.trades=$trades" "--generator.rps=$rps")
if [[ -n $amounts ]]; then
  if [[ ! $amounts =~ ^([1-9][0-9]{0,17})-([1-9][0-9]{0,17})$ ]] || (( BASH_REMATCH[1] > BASH_REMATCH[2] )); then
    fail_usage "--amounts must be MIN-MAX with 1 <= MIN <= MAX, got '$amounts'"
  fi
  generator_options+=("--generator.minAmount=${BASH_REMATCH[1]}" "--generator.maxAmount=${BASH_REMATCH[2]}")
fi
if [[ $zipf == true ]]; then
  generator_options+=(--generator.distribution=zipf)
fi

command -v python3 > /dev/null || { echo "e2e.sh: python3 is needed to read the report" >&2; exit 1; }
docker compose version > /dev/null 2>&1 || { echo "e2e.sh: needs Docker with Compose (docker compose)" >&2; exit 1; }

# --- The scenario ------------------------------------------------------------------------------

log() {
  printf '[e2e %s] %s\n' "$(date +%H:%M:%S)" "$*"
}

# infra/.env is not read (--env-file /dev/null): the options alone define the scenario.
compose() {
  docker compose --file "$compose_file" --env-file /dev/null "$@"
}

# For the same reason, drop the variables that define a scenario if they come from the shell:
# the ones that select implementations, the model or the topics, and the Compose variables that
# choose another project or more services (COMPOSE_PROFILES=tools would start a generator).
# Others, such as LOG_LEVEL or KAFKA_HEAP_OPTS, still apply.
for variable in $(compgen -e); do
  case $variable in
    IMPL | TOOLS_IMPL | AGGREGATOR*_IMPL | VALIDATOR*_IMPL | MODEL | KAFKA_TRANSACTIONS | \
    BANK_COUNT | TRADES_PARTITIONS | COMPOSE_PROJECT_NAME | COMPOSE_PROFILES | COMPOSE_FILE)
      unset "$variable" ;;
  esac
done

export IMPL=$impl MODEL=$model KAFKA_TRANSACTIONS=$transactions
[[ -z $aggregators ]] || export AGGREGATOR_IMPL=$aggregators
[[ -z $validators ]] || export VALIDATOR_IMPL=$validators
[[ -z $tools ]] || export TOOLS_IMPL=$tools
[[ -z $banks ]] || export BANK_COUNT=$banks
[[ -z $trades_partitions ]] || export TRADES_PARTITIONS=$trades_partitions

# The compose file may have any number of sites (see infra/generate-compose.sh).
services=$(compose config --services) || { echo "e2e.sh: $compose_file is not valid" >&2; exit 1; }
sites=$(grep -c '^validator-' <<< "$services" || true)
(( sites >= 1 )) || { echo "e2e.sh: $compose_file has no validators" >&2; exit 1; }
if [[ $chaos == true ]] && (( sites < 2 )); then
  fail_usage "--chaos needs at least 2 sites, $compose_file has $sites"
fi

# Odd sites in Java, even sites in Go.
mix_implementations() {  # mix_implementations AGGREGATOR|VALIDATOR
  local site
  for (( site = 1; site <= sites; site++ )); do
    if (( site % 2 == 1 )); then export "$1_${site}_IMPL=java"; else export "$1_${site}_IMPL=go"; fi
  done
}
[[ $mixed_aggregators == false ]] || mix_implementations AGGREGATOR
[[ $mixed_validators == false ]] || mix_implementations VALIDATOR

# e.g. java-concurrent-chaos, go-sequential-amounts500-5000, agg-mixed+val-go+tools-go-concurrent-notx
scenario_name() {
  local aggregator_impl=${aggregators:-$impl} validator_impl=${validators:-$impl} tools_impl=${tools:-$impl}
  local name
  if [[ $mixed_aggregators == true ]]; then aggregator_impl=mixed; fi
  if [[ $mixed_validators == true ]]; then validator_impl=mixed; fi
  if [[ $aggregator_impl == "$validator_impl" && $validator_impl == "$tools_impl" ]]; then
    name=$tools_impl
  else
    name=agg-$aggregator_impl+val-$validator_impl+tools-$tools_impl
  fi
  name+=-$model
  if [[ $transactions == false ]]; then name+=-notx; fi
  if [[ -n $amounts ]]; then name+=-amounts$amounts; fi
  if [[ $zipf == true ]]; then name+=-zipf; fi
  if [[ -n $banks ]]; then name+=-banks$banks; fi
  if [[ -n $trades_partitions ]]; then name+=-trades-partitions$trades_partitions; fi
  if [[ $chaos == true ]]; then name+=-chaos; fi
  echo "$name"
}

# e.g. "20000 trades at 2000/s, amounts 500-5000, Zipf-distributed accounts"
describe_trades() {
  local description="$trades trades at $rps/s"
  if [[ -n $amounts ]]; then description+=", amounts $amounts"; fi
  if [[ $zipf == true ]]; then description+=", Zipf-distributed accounts"; fi
  echo "$description"
}

describe_scenario() {
  local aggregator_impl=${aggregators:-$impl} validator_impl=${validators:-$impl} kafka_transactions=on description
  if [[ $mixed_aggregators == true ]]; then aggregator_impl="java (odd sites) and go (even sites)"; fi
  if [[ $mixed_validators == true ]]; then validator_impl="java (odd sites) and go (even sites)"; fi
  if [[ $transactions == false ]]; then kafka_transactions=off; fi
  description="aggregators $aggregator_impl, validators $validator_impl, tools ${tools:-$impl};"
  description+=" $model model, Kafka transactions $kafka_transactions;"
  description+=" ${banks:-$sites} banks, ${trades_partitions:-$sites} trades partitions; $(describe_trades)"
  if [[ $chaos == true ]]; then description+="; chaos"; fi
  echo "$description"
}

scenario=$(scenario_name)
results_dir=$repo_root/results/e2e/$(date -u +%Y%m%dT%H%M%SZ)-$scenario
results_shown=${results_dir#"$repo_root"/}   # the same, relative to the repo, for messages
mkdir -p "$results_dir"

# --- Steps -------------------------------------------------------------------------------------

cluster_started=false   # true once there may be containers to save logs from and remove
chaos_pid=""            # the background chaos job, while it runs
failure=""              # what went wrong, for the verdict
summary=""              # the report's key numbers, for the verdict

# Stops the test. The EXIT trap saves the logs, tears down and prints the verdict.
fail_test() {
  failure=$*
  exit 1
}

# Joins the lines of its input with ", ".
comma_list() {
  awk 'NR > 1 { printf ", " } { printf "%s", $0 } END { print "" }'
}

# Removes the cluster and its data. With the tools profile and --remove-orphans it also removes
# a generator or report that is still running ("run" makes one-off containers, which only
# --remove-orphans removes), and with it the network. The containers are killed at once: they
# are deleted with all their data, so nothing needs a clean shutdown.
remove_cluster() {
  compose --profile tools --progress quiet down --volumes --remove-orphans --timeout 0
}

# "<image> <service>" for each image the scenario builds. Building one service per image is
# enough; Compose would build a shared image once for every service that uses it.
images_to_build() {
  compose config --format json | python3 -c '
import json, sys
services = json.load(sys.stdin)["services"]
first_service_of_image = {}
for name in sorted(services):
    if "build" in services[name]:
        first_service_of_image.setdefault(services[name]["image"], name)
for image, service in first_service_of_image.items():
    print(image, service)'
}

build_images() {
  local plan image service images=() services=()
  plan=$(images_to_build) || fail_test "could not read the images to build from $compose_file"
  while read -r image service; do
    images+=("$image")
    services+=("$service")
  done <<< "$plan"
  log "Building ${images[*]} (log: $results_shown/build.log)"
  if ! compose build "${services[@]}" > "$results_dir/build.log" 2>&1; then
    tail -n 40 "$results_dir/build.log"
    fail_test "the images could not be built (see build.log)"
  fi
}

start_cluster() {
  log "Starting Kafka, then init, then the aggregators and validators"
  cluster_started=true
  # "up" waits for every broker to be healthy before it runs init, and for init to complete
  # successfully before it starts the aggregators and validators. It fails if either doesn't.
  if ! compose up -d > "$results_dir/up.log" 2>&1; then
    tail -n 5 "$results_dir/up.log"
    echo "--- init:"
    compose logs --no-color --tail 40 init 2>&1 || true
    fail_test "the cluster did not start (see up.log and init.log)"
  fi
  local init_state
  init_state=$(compose ps -a --format '{{.State}} {{.ExitCode}}' init)
  [[ $init_state == "exited 0" ]] || fail_test "init did not complete successfully: $init_state"
}

# "<group> <state> <members>" of the two consumer groups, e.g. "aggregators Stable 3", one line
# each in that order. A group that doesn't exist yet is missing; nothing if Kafka can't answer.
group_states() {
  compose exec -T kafka-1 env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka-1:19092 --describe --all-groups --state < /dev/null 2> /dev/null |
    awk '$1 == "aggregators" || $1 == "validators" { print $1, $(NF - 1), $NF }' | sort || true
}

# What group_states prints once both groups are stable with the given number of members.
stable_groups() {  # stable_groups MEMBERS
  printf 'aggregators Stable %s\nvalidators Stable %s' "$1" "$1"
}

# The output of group_states on one line, e.g. "aggregators Stable 3, validators Stable 2".
describe_groups() {  # describe_groups GROUP_STATES
  if [[ -n $1 ]]; then comma_list <<< "$1"; else echo "no consumer groups"; fi
}

# "<service> <state> <restarts>" of every aggregator and validator, e.g. "validator-2 running 1".
process_states() {
  compose ps -aq | xargs docker inspect \
      --format '{{index .Config.Labels "com.docker.compose.service"}} {{.State.Status}} {{.RestartCount}}' 2> /dev/null |
    awk '$1 ~ /^(aggregator|validator)-/' || true
}

# Aggregators and validators that exited or were restarted since they started.
crashed_processes() {
  process_states | awk '$2 != "running" || $3 > 0 { print $1 }'
}

# The trades are only sent once every aggregator and validator has joined its consumer group,
# so the test doesn't start with partitions moving between processes that are still starting.
wait_for_processes() {
  local deadline=$(( SECONDS + STARTUP_TIMEOUT_S )) groups crashed
  log "Waiting for $sites aggregators and $sites validators to join their consumer groups"
  while true; do
    read -ra crashed <<< "$(crashed_processes | tr '\n' ' ')"
    if (( ${#crashed[@]} > 0 )); then
      compose logs --no-color --tail 20 "${crashed[@]}" 2>&1 || true
      fail_test "crashed while starting: ${crashed[*]}"
    fi
    groups=$(group_states)
    [[ $groups != "$(stable_groups "$sites")" ]] || return 0
    if (( SECONDS >= deadline )); then
      fail_test "the consumer groups did not reach $sites members each within $STARTUP_TIMEOUT_S s" \
        "($(describe_groups "$groups"))"
    fi
    sleep 2
  done
}

chaos_log() {
  log "chaos: $*" | tee -a "$results_dir/chaos.log"
}

# The last step the chaos started, as chaos_log wrote it.
last_chaos_step() {
  sed -n 's/^\[e2e [0-9:]*\] chaos: //p' "$results_dir/chaos.log" | tail -n 1
}

# Sleeps until $SECONDS reaches the given value; returns at once if it already has.
sleep_until() {  # sleep_until SECONDS
  if (( $1 > SECONDS )); then sleep $(( $1 - SECONDS )); fi
}

# The paused processes miss their session timeout, so both groups drop them and the other
# processes take over their partitions, fencing them. Unless that happens before they wake up,
# they are not zombies, and the test doesn't test what it claims. A group is only stable again
# once every member it still has rejoined, and a paused process can't, so "Stable" with one
# member fewer means that the partitions have moved to the others.
wait_for_takeover() {  # wait_for_takeover DEADLINE_SECONDS
  local groups
  while true; do
    groups=$(group_states)
    if [[ $groups == "$(stable_groups $(( sites - 1 )))" ]]; then
      chaos_log "the others took over from ${paused[*]}: $(describe_groups "$groups")"
      return 0
    fi
    if (( SECONDS >= $1 )); then
      chaos_log "the others did not take over from ${paused[*]} in time: $(describe_groups "$groups")"
      return 1
    fi
    sleep 1
  done
}

# Runs in the background while the generator sends trades. A SIGKILLed process loses all it
# hadn't committed and must recover from Kafka. A paused process misses its session timeout,
# so its partitions move to the others, and nothing it does afterwards can be committed: the
# new owners of the paused validator's banks fence its producers, and the paused aggregator's
# consumer-group generation is outdated, so Kafka refuses its offsets (an open transaction of
# it ends when it wakes up, or when the broker aborts it after the transaction timeout,
# kafka.transactionTimeoutMs). When a paused process wakes
# up, it either finds itself fenced and exits (Docker restarts it) or rejoins its group.
run_chaos() {
  local paused_at
  sleep "$CHAOS_START_S"
  chaos_log "SIGKILL ${killed[*]}"
  compose kill -s SIGKILL "${killed[@]}" >> "$results_dir/chaos.log" 2>&1
  chaos_log "pause ${paused[*]} for $PAUSED_FOR_S s (the session timeout is 10 s)"
  compose pause "${paused[@]}" >> "$results_dir/chaos.log" 2>&1
  paused_at=$SECONDS
  sleep "$KILLED_FOR_S"
  # "restart --no-deps", because "start" would also run init again (a dependency).
  chaos_log "restart ${killed[*]}"
  compose restart --no-deps "${killed[@]}" >> "$results_dir/chaos.log" 2>&1
  # The takeover must be seen while the paused processes sleep, so the unpause waits for it.
  wait_for_takeover $(( paused_at + PAUSED_FOR_S - 2 ))
  sleep_until $(( paused_at + PAUSED_FOR_S ))
  chaos_log "unpause ${paused[*]}"
  compose unpause "${paused[@]}" >> "$results_dir/chaos.log" 2>&1
}

run_generator() {
  local generator_status=0 chaos_status=0
  log "Running the trade generator: $(describe_trades)"
  if [[ $chaos == true ]]; then
    run_chaos &
    chaos_pid=$!
  fi
  compose run --rm -T generator "${generator_options[@]}" < /dev/null 2>&1 |
    tee "$results_dir/generator.log" || generator_status=$?
  if [[ -n $chaos_pid ]]; then
    wait "$chaos_pid" || chaos_status=$?
    chaos_pid=""
  fi
  (( generator_status == 0 )) || fail_test "the trade generator failed with exit status $generator_status"
  (( chaos_status == 0 )) || fail_test "a chaos step failed: $(last_chaos_step) (see chaos.log)"
}

# Prints one top-level value of a REPORT_JSON payload, e.g. report_value "$json" generated.
report_value() {  # report_value JSON KEY
  python3 -c 'import json, sys; print(json.loads(sys.argv[1])[sys.argv[2]])' "$1" "$2"
}

# Prints "<result> <key numbers>" for a REPORT_JSON payload, e.g. "PASS 20000 generated, ...".
summarize_report() {  # summarize_report JSON
  python3 -c '
import json, sys
report = json.loads(sys.argv[1])
latency, validation = report["latencyMs"], report["validationLatencyMs"]
parts = [
    "{generated} generated, {accepted} accepted, {rejected} rejected, {completed} completed, "
    "{inFlight} in flight".format(**report),
    "{:.0f} trades/s".format(report["throughputTps"]),
    "latency p50 {p50:.0f} ms, p95 {p95:.0f} ms, p99 {p99:.0f} ms, max {max:.0f} ms".format(**latency),
    "validation p95 {p95:.0f} ms".format(**validation),
]
failed = ["{} ({})".format(check["name"], check.get("detail", "")) for check in report["checks"] if not check["passed"]]
if failed:
    parts.append("failed: " + ", ".join(failed))
print(report["result"], "; ".join(parts))' "$1"
}

run_report() {
  local report_status=0 report_json line generated rejected
  log "Running the report (it waits up to $(( REPORT_TIMEOUT_MS / 1000 )) s for trades in flight)"
  compose run --rm -T report --report.json=true "--report.timeoutMs=$REPORT_TIMEOUT_MS" < /dev/null 2>&1 |
    tee "$results_dir/report.log" || report_status=$?
  report_json=$(sed -n 's/^.*REPORT_JSON //p' "$results_dir/report.log" | tr -d '\r' | tail -n 1)
  [[ -n $report_json ]] || fail_test "the report printed no REPORT_JSON line (exit status $report_status)"
  printf '%s\n' "$report_json" > "$results_dir/report.json"
  line=$(summarize_report "$report_json") || fail_test "the REPORT_JSON line can't be read (see report.json)"
  summary=${line#* }
  [[ ${line%% *} == PASS ]] || fail_test "the report says ${line%% *}"
  (( report_status == 0 )) || fail_test "the report says PASS but exited with status $report_status"
  # The report's checks only cover the trades it found on the trades topic: on a fresh cluster
  # these must be exactly the trades the generator was asked to send.
  generated=$(report_value "$report_json" generated) || fail_test "the REPORT_JSON line has no generated count"
  [[ $generated == "$trades" ]] || fail_test "the report found $generated generated trades instead of $trades"
  if [[ -n $amounts ]]; then
    rejected=$(report_value "$report_json" rejected) || fail_test "the REPORT_JSON line has no rejected count"
    is_count "$rejected" || fail_test "no trade was rejected, although --amounts $amounts should make balances run short"
  fi
}

# Processes that restarted more often than expected, e.g. "validator-3: restarts 2". With
# --chaos, a paused process may find itself fenced when it wakes up and exit, and Docker
# restarts it once. No other process restarts: the SIGKILLed ones are restarted by the test,
# which Docker doesn't count.
unexplained_restarts() {  # unexplained_restarts PROCESS_STATES
  local allowed_once=""
  if [[ $chaos == true ]]; then allowed_once=" ${paused[*]} "; fi
  awk -v allowed_once="$allowed_once" '
    { allowed = index(allowed_once, " " $1 " ") ? 1 : 0 }
    $3 > allowed { print $1 ": restarts " $3 }' <<< "$1"
}

# After the report, every aggregator and validator must be running and back in its consumer
# group. A process that keeps failing could otherwise go unnoticed: the others take over its
# partitions, so all trades still complete.
check_recovered() {
  local deadline=$(( SECONDS + RECOVERY_TIMEOUT_S )) states restarted running groups
  log "Checking that every aggregator and validator is running and in its consumer group again"
  while true; do
    states=$(process_states)
    restarted=$(unexplained_restarts "$states")
    [[ -z $restarted ]] || fail_test "restarted unexpectedly: $(comma_list <<< "$restarted")"
    running=$(awk '$2 == "running" { n++ } END { print n + 0 }' <<< "$states")
    groups=$(group_states)
    if (( running == 2 * sites )) && [[ $groups == "$(stable_groups "$sites")" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      fail_test "not recovered $RECOVERY_TIMEOUT_S s after the report:" \
        "$running of $(( 2 * sites )) aggregators and validators running" \
        "($(describe_groups "$groups"))"
    fi
    sleep 2
  done
}

save_logs() {
  local service
  log "Saving the logs to $results_shown"
  compose --profile tools config > "$results_dir/compose.yml" 2>&1
  {
    compose ps -a
    echo
    # Restart counts show which processes crashed, or exited after being fenced.
    compose ps -aq | xargs docker inspect --format \
      '{{index .Config.Labels "com.docker.compose.service"}}: restarts {{.RestartCount}}, {{.State.Status}}, exit status {{.State.ExitCode}}, image {{.Image}}'
  } > "$results_dir/containers.txt" 2>&1
  for service in $(compose ps -a --services); do
    case $service in
      generator | report) ;;   # their output was saved while they ran
      *) compose logs --no-color --timestamps "$service" > "$results_dir/$service.log" 2>&1 ;;
    esac
  done
}

# Runs on every exit, also after a failure or Ctrl-C. The cleanup is best effort and must get
# to the end, even if a step fails or nobody reads the output anymore (Ctrl-C also stops a
# "| tee" that reads it, and writing to it would then kill the script with SIGPIPE).
on_exit() {
  local status=$? verdict=FAIL details service
  set +e
  trap - EXIT
  trap '' PIPE
  if [[ -n $chaos_pid ]]; then
    kill "$chaos_pid" 2> /dev/null
    wait "$chaos_pid" 2> /dev/null
  fi
  # A chaos step that failed or was interrupted may have left processes paused.
  if [[ $chaos == true && $cluster_started == true ]]; then
    for service in "${paused[@]}"; do
      compose unpause "$service" > /dev/null 2>&1
    done
  fi
  if [[ $cluster_started == true ]]; then
    save_logs
    if [[ $keep == true ]]; then
      log "The cluster keeps running. Remove it with: docker compose -f infra/docker-compose.yml --profile tools down -v"
    else
      log "Removing the cluster"
      remove_cluster
    fi
  fi
  if (( status == 0 )); then verdict=PASS; fi
  details=$summary
  if [[ -n $failure ]]; then details+="${details:+; }$failure"; fi
  echo "$verdict $scenario: ${details:-exit status $status} ($results_shown)"
  exit "$status"
}

trap on_exit EXIT
trap 'failure=interrupted; exit 130' INT
trap 'failure=terminated; exit 143' TERM

{
  echo "Scenario $scenario: $(describe_scenario)"
  echo
  echo "Variables that select the implementations, the model and the topics:"
  env | grep -E '^(IMPL|TOOLS_IMPL|AGGREGATOR[_0-9]*_IMPL|VALIDATOR[_0-9]*_IMPL|MODEL|KAFKA_TRANSACTIONS|BANK_COUNT|TRADES_PARTITIONS)=' | sort
  echo
  echo "Trade generator: ${generator_options[*]}"
} > "$results_dir/scenario.txt"

log "Scenario $scenario: $(describe_scenario)"
log "Results: $results_shown"
log "Removing any previous cluster"
remove_cluster || fail_test "could not remove the previous cluster"
build_images
start_cluster
wait_for_processes
run_generator
run_report
check_recovered
