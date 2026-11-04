#!/usr/bin/env bash
#
# Writes the Docker Compose file of a local cluster (see docs/deployment).
#
# The cluster has SITES sites. A site is one bank's infrastructure: a Kafka node (kafka-<i>),
# an aggregator (aggregator-<i>) and a validator (validator-<i>), all with site ID <i>.
# The committed infra/docker-compose.yml is this script's output for the defaults: after
# changing the script, run it without options and commit both files.
#
# Usage: infra/generate-compose.sh [--sites=N] [--controllers=N] [--replication-factor=N] [--output=FILE]
#
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: infra/generate-compose.sh [options]

Writes the Docker Compose file of a local cluster of SITES sites. Each site has one Kafka
node, one aggregator and one validator, and by default owns one bank.

Options (the first three can also be set as environment variables):
  --sites=N               SITES               number of sites (default 3)
  --controllers=N         CONTROLLERS         the first N Kafka nodes are broker + controller, the
                                              others are brokers only; 1 <= N <= SITES
                                              (default 3, or SITES if that is smaller)
  --replication-factor=N  REPLICATION_FACTOR  replication of every topic, including Kafka's
                                              internal ones; 1 <= N <= SITES (default 1)
  --output=FILE                               the file to write (default infra/docker-compose.yml)
  -h, --help                                  show this help
EOF
}

# Bad options or values: exit 2, like the dp command does.
fail() {
  echo "generate-compose.sh: $*" >&2
  echo "Run 'infra/generate-compose.sh --help' for usage." >&2
  exit 2
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(dirname "$script_dir")

# Every node of a cluster is formatted with the same cluster ID. Any base64url-encoded UUID
# works; this one came from `kafka-storage.sh random-uuid`.
readonly CLUSTER_ID=Y3IjHZIoRqOksnTfvL7joQ

sites=${SITES:-3}
controllers=${CONTROLLERS:-}
replication_factor=${REPLICATION_FACTOR:-1}
output=$script_dir/docker-compose.yml

# Options: --name=value or --name value.
while (( $# > 0 )); do
  option=$1
  shift
  case $option in
    -h | --help) usage; exit 0 ;;
    --*=*) name=${option%%=*}; value=${option#*=} ;;
    --*)
      (( $# > 0 )) || fail "$option needs a value"
      name=$option; value=$1
      shift ;;
    *) fail "unexpected argument: $option" ;;
  esac
  case $name in
    --sites) sites=$value ;;
    --controllers) controllers=$value ;;
    --replication-factor) replication_factor=$value ;;
    --output) output=$value ;;
    *) fail "unknown option: $name" ;;
  esac
done

is_positive_integer() {
  [[ $1 =~ ^[1-9][0-9]*$ ]]
}

is_positive_integer "$sites" || fail "sites must be a positive integer, got '$sites'"
if [[ -z $controllers ]]; then
  controllers=$(( sites < 3 ? sites : 3 ))
fi
is_positive_integer "$controllers" || fail "controllers must be a positive integer, got '$controllers'"
(( controllers <= sites )) || fail "controllers ($controllers) must not exceed sites ($sites)"
is_positive_integer "$replication_factor" || fail "replication factor must be a positive integer, got '$replication_factor'"
(( replication_factor <= sites )) || fail "replication factor ($replication_factor) must not exceed sites ($sites)"
[[ ! -d $output ]] || fail "$output is a directory; give a file name"
[[ -d $(dirname "$output") ]] || fail "the directory of $output does not exist"
# The KRaft quorum needs a majority of the controllers, so an even number of them survives no
# more failures than one controller fewer.
if (( controllers % 2 == 0 )); then
  echo "Note: $controllers controllers need $(( controllers / 2 + 1 )) for a quorum, so the cluster" \
    "survives the loss of $(( (controllers - 1) / 2 )), no more than with $(( controllers - 1 ))." >&2
fi

# The images are built with the repo root as context. The committed file names it relative
# to its own directory, so it works in any checkout; a file written elsewhere gets the
# absolute path.
if [[ $(cd "$(dirname "$output")" && pwd -P) == "$script_dir" ]]; then
  build_context=..
else
  build_context=$repo_root
fi

# The transaction coordinator only records a transaction's state while this many replicas of
# its __transaction_state partition are in sync. With replication, 2 keeps that state on two
# nodes. With a replication factor of exactly 2, it also means that a node that is down stops
# every Kafka transaction whose state it holds a replica of: durability before availability.
# The app's own topics keep Kafka's default min.insync.replicas of 1.
if (( replication_factor == 1 )); then
  transaction_state_min_isr=1
else
  transaction_state_min_isr=2
fi

# --- Helpers -------------------------------------------------------------------------------

# The Kafka bootstrap list of site $1: every node's internal listener, the site's own first.
bootstrap_servers() {
  local own=$1 i list="kafka-$1:19092"
  for (( i = 1; i <= sites; i++ )); do
    if (( i != own )); then list+=",kafka-$i:19092"; fi
  done
  echo "$list"
}

# The static controller quorum: 1@kafka-1:9093,2@kafka-2:9093,...
controller_voters() {
  local i list=""
  for (( i = 1; i <= controllers; i++ )); do
    list+="${list:+,}$i@kafka-$i:9093"
  done
  echo "$list"
}

# Which implementation (java or go) runs a service is a Compose variable with fallbacks, so
# it can be chosen per site, per role or for everything, e.g. for validator-2:
#   ${VALIDATOR_2_IMPL:-${VALIDATOR_IMPL:-${IMPL:-java}}}
# shellcheck disable=SC2016  # these are Compose variables, written to the file as they are
site_impl() {  # site_impl AGGREGATOR|VALIDATOR SITE
  printf '${%s_%s_IMPL:-${%s_IMPL:-${IMPL:-java}}}' "$1" "$2" "$1"
}
# shellcheck disable=SC2016
readonly TOOLS_IMPL='${TOOLS_IMPL:-${IMPL:-java}}'

# The image of an implementation and how to build it.
image_lines() {  # image_lines IMPL_VARIABLE
  cat <<EOF
    image: decoupled-payments-$1
    build:
      context: $build_context
      dockerfile: $1/Dockerfile
EOF
}

# --- The file, section by section ------------------------------------------------------------

header() {
  local i
  cat <<EOF
# GENERATED by infra/generate-compose.sh: don't edit, change the script and run it again.
#   infra/generate-compose.sh           writes this file (the defaults)
#   infra/generate-compose.sh --help    other cluster shapes
# This file: --sites=$sites --controllers=$controllers --replication-factor=$replication_factor
#
# A local cluster of $sites sites (docs/deployment). Site <i> is the Kafka node kafka-<i>, the
# aggregator aggregator-<i> and the validator validator-<i>, which by default owns one bank.
#
# From the repo root:
#   docker compose -f infra/docker-compose.yml up -d --build   Kafka, init, aggregators, validators
#   docker compose -f infra/docker-compose.yml run --rm generator --generator.trades=20000
#   docker compose -f infra/docker-compose.yml run --rm report
#   docker compose -f infra/docker-compose.yml --profile tools down -v
#                                                  stops and deletes the cluster and its data
#
# Kafka keeps its data inside each node's container, so every "up" after "down" starts with
# empty topics, and no volumes are created (see x-kafka). A generator or report started with
# "run" is a one-off container: if one is still running, "down" removes it only with
# --remove-orphans.
#
# Optional variables (in the shell or in infra/.env):
#   IMPL=java|go                                 implementation of every role (default java)
#   AGGREGATOR_IMPL, VALIDATOR_IMPL, TOOLS_IMPL  implementation of one role (tools: init,
#                                                generator, report)
#   AGGREGATOR_<i>_IMPL, VALIDATOR_<i>_IMPL      implementation of one site's process
#   MODEL=concurrent|sequential, KAFKA_TRANSACTIONS=true|false, LOG_LEVEL=info|debug|warn|error
#   BANK_COUNT, TRADES_PARTITIONS                number of banks and of trades partitions
#                                                (default $sites, one of each per site)
#   KAFKA_IMAGE, KAFKA_HEAP_OPTS
#
# Kafka from the host (bound to 127.0.0.1 only: Kafka runs without authentication):
EOF
  for (( i = 1; i <= sites; i++ )); do
    echo "#   localhost:$(( 9091 + i ))   kafka-$i"
  done
  cat <<EOF
# From the containers: $(bootstrap_servers 1)

name: decoupled-payments

EOF
}

shared_settings() {
  # $$ is a literal $ for Compose: KAFKA_NODE_ID is expanded by the shell in the container.
  cat <<EOF
# Settings of every Kafka node. Each node adds its ID, its roles and its listeners.
x-kafka: &kafka
  image: \${KAFKA_IMAGE:-apache/kafka:4.3.2}
  # The image declares three volumes that this cluster doesn't use: Kafka keeps its data in
  # /tmp/kafka-logs inside the container (KAFKA_LOG_DIRS isn't set). Empty in-memory mounts
  # take their place, so Docker creates no anonymous volumes that could be left behind.
  tmpfs: [/var/lib/kafka/data, /etc/kafka/secrets, /mnt/shared/config]
  healthcheck:
    # Healthy once the broker answers requests and is in the cluster metadata, which leaves
    # out brokers that haven't registered or are fenced. The check's JVM gets a small heap.
    test: ["CMD-SHELL", "KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 | grep -q \"(id: \$\${KAFKA_NODE_ID} \""]
    start_period: 120s
    start_interval: 2s
    interval: 30s
    timeout: 20s
    retries: 3

x-kafka-environment: &kafka-environment
  CLUSTER_ID: $CLUSTER_ID
  KAFKA_CONTROLLER_QUORUM_VOTERS: $(controller_voters)
  # INTERNAL: other nodes and the containers. EXTERNAL: clients on the host. CONTROLLER: the quorum.
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
  KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  # init creates every topic with its own settings; a mistyped topic name must fail.
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_DEFAULT_REPLICATION_FACTOR: $replication_factor
  # Kafka's internal topics: consumer group offsets and the state of Kafka transactions.
  # Transaction state is only written while MIN_ISR replicas are in sync (2 when replicated,
  # so with a replication factor of 2, a node that is down stops the transactions whose state
  # it replicates).
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: $replication_factor
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: $replication_factor
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: $transaction_state_min_isr
  # Assign partitions as soon as a consumer joins, instead of waiting 3 s for more members.
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  KAFKA_HEAP_OPTS: \${KAFKA_HEAP_OPTS:--Xmx512m -Xms512m}

# Settings of every dp process (see config/default.yaml). Aggregators and validators add
# their site ID and put their own site's Kafka node first.
x-app-environment: &app-environment
  DP_MODEL: \${MODEL:-concurrent}
  DP_KAFKA_TRANSACTIONS: \${KAFKA_TRANSACTIONS:-true}
  # Only init reads these two, to create the topics; the other roles find the banks in
  # accountInfo. By default each site's validator owns one bank and each aggregator reads
  # one trades partition.
  DP_BANK_COUNT: \${BANK_COUNT:-$sites}
  DP_TOPICS_TRADES_PARTITIONS: \${TRADES_PARTITIONS:-$sites}
  DP_TOPICS_REPLICATION_FACTOR: $replication_factor
  DP_KAFKA_BOOTSTRAP_SERVERS: $(bootstrap_servers 1)
  DP_LOG_LEVEL: \${LOG_LEVEL:-info}

# An aggregator or validator starts once init has created the topics. Docker restarts it
# when it exits with an error, e.g. a validator whose bank was taken over by another one:
# it rebuilds its state from Kafka on every start.
x-site-process: &site-process
  restart: on-failure
  depends_on:
    init:
      condition: service_completed_successfully

services:
EOF
}

kafka_node() {  # kafka_node SITE
  local i=$1 roles=broker listeners="INTERNAL://:19092,EXTERNAL://:9092"
  if (( i <= controllers )); then
    roles=broker,controller
    listeners+=",CONTROLLER://:9093"
  fi
  cat <<EOF
  kafka-$i:
    <<: *kafka
    ports: ["127.0.0.1:$(( 9091 + i )):9092"]
    environment:
      <<: *kafka-environment
      KAFKA_NODE_ID: $i
      KAFKA_PROCESS_ROLES: $roles
      KAFKA_LISTENERS: $listeners
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-$i:19092,EXTERNAL://localhost:$(( 9091 + i ))

EOF
}

init_service() {
  local i
  echo "  # Creates the topics and the accounts, then exits."
  echo "  init:"
  image_lines "$TOOLS_IMPL"
  cat <<EOF
    command: ["init"]
    restart: "no"
    environment: *app-environment
    depends_on:
EOF
  for (( i = 1; i <= sites; i++ )); do
    echo "      kafka-$i:"
    echo "        condition: service_healthy"
  done
  echo
}

site_process() {  # site_process aggregator|validator SITE
  local role=$1 i=$2
  local role_variable
  role_variable=$(tr '[:lower:]' '[:upper:]' <<< "$role")
  echo "  $role-$i:"
  echo "    <<: *site-process"
  image_lines "$(site_impl "$role_variable" "$i")"
  cat <<EOF
    command: ["$role"]
    environment:
      <<: *app-environment
      DP_SITE_ID: $i
      DP_KAFKA_BOOTSTRAP_SERVERS: $(bootstrap_servers "$i")

EOF
}

tool() {  # tool generator|report
  local role=$1
  echo "  $role:"
  image_lines "$TOOLS_IMPL"
  cat <<EOF
    profiles: ["tools"]
    entrypoint: ["dp", "$role"]
    environment: *app-environment
EOF
}

tools() {
  cat <<'EOF'
  # Tools only run on request: docker compose run --rm <tool> [flags]. The role is part of
  # the entrypoint, so the flags given to "run" are added to it instead of replacing it.
EOF
  tool generator
  echo
  tool report
}

compose_file() {
  local i
  header
  shared_settings
  for (( i = 1; i <= sites; i++ )); do kafka_node "$i"; done
  init_service
  for (( i = 1; i <= sites; i++ )); do site_process aggregator "$i"; done
  for (( i = 1; i <= sites; i++ )); do site_process validator "$i"; done
  tools
}

# Write to a temporary file first, so a failure never leaves a half-written compose file.
temporary=$(mktemp "$output.XXXXXX")
trap 'rm -f "$temporary"' EXIT
compose_file > "$temporary"
chmod 644 "$temporary"   # mktemp makes it private to its owner
mv "$temporary" "$output"
echo "Wrote $output (sites $sites, controllers $controllers, replication factor $replication_factor)." >&2
