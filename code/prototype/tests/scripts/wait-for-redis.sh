#!/bin/sh
# wait-for-redis.sh host port cmd

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

>&2 echo "waiting for Redis on $host:$port"
until redis-cli -h $host -p $port ping | grep -q PONG; do
  >&2 echo "Redis is unavailable - sleeping"
  sleep 1
done

>&2 echo "Redis is up - executing command '$cmd'"
exec $cmd
