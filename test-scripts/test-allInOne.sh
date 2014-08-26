#!/bin/bash

# example INDEX="http://141.76.47.133:8765"
INDEX=""

if [[ $INDEX == "" ]]; then
    echo "no IndexServer url set"
    exit 1
fi

if [[ $1 == "" ]]; then
    echo "no output path given"
    exit 1
fi

re='^[0-9]+$'
if ! [[ $2 =~ $re ]] ; then
   echo "error: $2 not a number" >&2;
   exit 1
fi

database_file=$1
num_index_candidates=$2
test_class=rea.test.$3


test-scripts/run.sh $test_class -i $INDEX -f "$database_file" -u $num_index_candidates
