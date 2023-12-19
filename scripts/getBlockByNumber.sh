#!/bin/bash
#
# getBlockByNumber.sh <url> <num-threads> <count> <timeout-sec> <verbose>

MIN_NUM=36640000
MAX_NUM=36680000
URL=${1:-http://localhost:8588}
NUM_THREADS=${2:-100}
COUNT=${3:-1000}
TIMEOUT=${4:-1}
VERBOSE=${5:-0}

cleanup() {
    pkill -P $$
    exit 1
}
trap cleanup INT

function get_block()
{
    TIX=$1
    IX=0
    while [ $IX -lt $COUNT ]; do
        NUM=$(($RANDOM % ($MAX_NUM - $MIN_NUM + 1) + $MIN_NUM))
        # NUM=$(($MIN_NUM + $IX))
        DATA='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["'$(printf "0x%x" $NUM)'",true],"id":'$NUM'}'
        curl -s -m $TIMEOUT -L -X POST -o /dev/null -H "Content-Type: application/json" \
           --data "${DATA}" $URL
        echo "$TIX/$IX: $NUM -> $?"
        IX=$(($IX + 1))
    done
}

i=0
while [ $i -lt $NUM_THREADS ]; do
    (get_block $i) &
    i=$(($i + 1))
done

wait

# EOF
