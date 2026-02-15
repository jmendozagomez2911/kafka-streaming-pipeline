#!/usr/bin/env bash
set -euo pipefail

# Kafka CLI Lab Helper
# - Works for localhost and for secure clusters (if you set CMD_CONFIG)
# - Keeps commands close to the Conduktor course scripts you have in your repo
#
# Usage examples:
#   ./kafka-cli-lab.sh topics list
#   ./kafka-cli-lab.sh topics create first_topic 3 1
#   ./kafka-cli-lab.sh topics describe first_topic
#   ./kafka-cli-lab.sh producer run first_topic
#   ./kafka-cli-lab.sh producer run-keys first_topic
#   ./kafka-cli-lab.sh consumer tail second_topic
#   ./kafka-cli-lab.sh consumer from-beginning second_topic
#   ./kafka-cli-lab.sh consumer debug second_topic
#   ./kafka-cli-lab.sh group consume third_topic my-first-application
#   ./kafka-cli-lab.sh groups list
#   ./kafka-cli-lab.sh groups describe my-first-application
#   ./kafka-cli-lab.sh offsets reset-earliest my-first-application third_topic --execute
#
# Environment variables:
#   BOOTSTRAP   (default: localhost:9092)
#   CMD_CONFIG  (optional, e.g.: playground.config)
#   TOPICS_CMD, PRODUCER_CMD, CONSUMER_CMD, GROUPS_CMD (override command names if needed)

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
CMD_CONFIG="${CMD_CONFIG:-}"

TOPICS_CMD="${TOPICS_CMD:-kafka-topics.sh}"
PRODUCER_CMD="${PRODUCER_CMD:-kafka-console-producer.sh}"
CONSUMER_CMD="${CONSUMER_CMD:-kafka-console-consumer.sh}"
GROUPS_CMD="${GROUPS_CMD:-kafka-consumer-groups.sh}"

common_flags() {
  # Adds --command-config only if CMD_CONFIG is set
  if [[ -n "$CMD_CONFIG" ]]; then
    echo "--command-config $CMD_CONFIG --bootstrap-server $BOOTSTRAP"
  else
    echo "--bootstrap-server $BOOTSTRAP"
  fi
}

die() { echo "Error: $*" >&2; exit 1; }

help() {
  cat <<EOF
Kafka CLI Lab Helper

BOOTSTRAP=$BOOTSTRAP
CMD_CONFIG=${CMD_CONFIG:-"(none)"}

Commands:
  topics list
  topics create <topic> [partitions] [replication_factor]
  topics describe <topic>
  topics delete <topic>

  producer run <topic>                    (interactive, Ctrl+C to exit)
  producer run-acks-all <topic>           (interactive)
  producer run-keys <topic>               (interactive key:value, key.separator=:)

  consumer tail <topic>                   (only new messages)
  consumer from-beginning <topic>         (read all available data)
  consumer debug <topic>                  (print timestamp/key/value/partition)

  group consume <topic> <group>           (consumer in a group)
  group consume-from-beginning <topic> <group>

  groups list
  groups describe <group>

  offsets reset-earliest <group> <topic> [--execute]
      Default is --dry-run unless you add --execute

Notes:
- On Windows without WSL2, use *.bat commands (override TOPICS_CMD, etc).
- Topic deletion requires broker setting delete.topic.enable=true.
- Reset offsets only when consumers are stopped.
EOF
}

ensure_args() {
  local need="$1"; shift
  local got="$#"
  [[ "$got" -ge "$need" ]] || die "Expected at least $need arguments, got $got. Run with no args for help."
}

topics_list() {
  # shellcheck disable=SC2086
  $TOPICS_CMD $(common_flags) --list
}

topics_create() {
  ensure_args 1 "$@"
  local topic="$1"
  local partitions="${2:-3}"
  local rf="${3:-1}"

  # shellcheck disable=SC2086
  $TOPICS_CMD $(common_flags) --create --topic "$topic" --partitions "$partitions" --replication-factor "$rf"
}

topics_describe() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $TOPICS_CMD $(common_flags) --describe --topic "$topic"
}

topics_delete() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $TOPICS_CMD $(common_flags) --delete --topic "$topic"
}

producer_run() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $PRODUCER_CMD $(common_flags) --topic "$topic"
}

producer_run_acks_all() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $PRODUCER_CMD $(common_flags) --topic "$topic" --producer-property acks=all
}

producer_run_keys() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $PRODUCER_CMD $(common_flags) --topic "$topic" --property parse.key=true --property key.separator=:
}

consumer_tail() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $CONSUMER_CMD $(common_flags) --topic "$topic"
}

consumer_from_beginning() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $CONSUMER_CMD $(common_flags) --topic "$topic" --from-beginning
}

consumer_debug() {
  ensure_args 1 "$@"
  local topic="$1"
  # shellcheck disable=SC2086
  $CONSUMER_CMD $(common_flags) \
    --topic "$topic" \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.value=true \
    --property print.partition=true \
    --from-beginning
}

group_consume() {
  ensure_args 2 "$@"
  local topic="$1"
  local group="$2"
  # shellcheck disable=SC2086
  $CONSUMER_CMD $(common_flags) --topic "$topic" --group "$group"
}

group_consume_from_beginning() {
  ensure_args 2 "$@"
  local topic="$1"
  local group="$2"
  # shellcheck disable=SC2086
  $CONSUMER_CMD $(common_flags) --topic "$topic" --group "$group" --from-beginning
}

groups_list() {
  # shellcheck disable=SC2086
  $GROUPS_CMD $(common_flags) --list
}

groups_describe() {
  ensure_args 1 "$@"
  local group="$1"
  # shellcheck disable=SC2086
  $GROUPS_CMD $(common_flags) --describe --group "$group"
}

offsets_reset_earliest() {
  ensure_args 2 "$@"
  local group="$1"
  local topic="$2"
  local mode="${3:---dry-run}"  # default dry-run; allow --execute
  [[ "$mode" == "--dry-run" || "$mode" == "--execute" ]] || die "Mode must be --dry-run (default) or --execute"

  # shellcheck disable=SC2086
  $GROUPS_CMD $(common_flags) --group "$group" --reset-offsets --to-earliest --topic "$topic" "$mode"
}

main() {
  [[ $# -gt 0 ]] || { help; exit 0; }

  local domain="$1"; shift
  case "$domain" in
    topics)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        list) topics_list ;;
        create) topics_create "$@" ;;
        describe) topics_describe "$@" ;;
        delete) topics_delete "$@" ;;
        *) die "Unknown topics action: $action" ;;
      esac
      ;;
    producer)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        run) producer_run "$@" ;;
        run-acks-all) producer_run_acks_all "$@" ;;
        run-keys) producer_run_keys "$@" ;;
        *) die "Unknown producer action: $action" ;;
      esac
      ;;
    consumer)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        tail) consumer_tail "$@" ;;
        from-beginning) consumer_from_beginning "$@" ;;
        debug) consumer_debug "$@" ;;
        *) die "Unknown consumer action: $action" ;;
      esac
      ;;
    group)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        consume) group_consume "$@" ;;
        consume-from-beginning) group_consume_from_beginning "$@" ;;
        *) die "Unknown group action: $action" ;;
      esac
      ;;
    groups)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        list) groups_list ;;
        describe) groups_describe "$@" ;;
        *) die "Unknown groups action: $action" ;;
      esac
      ;;
    offsets)
      ensure_args 1 "$@"
      local action="$1"; shift
      case "$action" in
        reset-earliest) offsets_reset_earliest "$@" ;;
        *) die "Unknown offsets action: $action" ;;
      esac
      ;;
    -h|--help|help)
      help
      ;;
    *)
      die "Unknown domain: $domain"
      ;;
  esac
}

main "$@"
