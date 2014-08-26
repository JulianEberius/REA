#!/bin/bash

# example INDEX="http://141.76.47.133:8765"
INDEX="http://141.76.47.133:8765"

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

if ! [[ $3 =~ $re ]] ; then
   echo "error: $3 not a number" >&2;
   exit 1
fi

database_file=$1
num_index_candidates=$2
search_space=$3
test_func=run_correctness

run_correctness() {
    for numCov in 5; do
        echo "running" $1 $2
        for file in `ls $3*`; do
            echo "    " $file
            test-scripts/run.sh rea.test.CorrectnessTest -e "$file" -a "$2" -c "$1" -n $numCov -p -i $INDEX -f "$database_file" -u $num_index_candidates -s $search_space
        done;
    done;
}


test_suite() {
    for i in 20a 20b 20c Random20a Random20b; do
        concept="company"
        files="data/companies$i"
        attr="revenues|revenue|sales"
        $test_func $concept "$attr" $files
        attr="established|founded"
        $test_func $concept "$attr" $files
        attr="employees"
        $test_func $concept "$attr" $files
    done

    concept="city"
    files="data/cities20a"
    attr="population|inhabitants"
    $test_func $concept "$attr" $files
    files="data/cities20b"
    $test_func $concept "$attr" $files

    concept="city"
    files="data/citiesRandom20a"
    attr="population|inhabitants"
    $test_func $concept "$attr" $files
    files="data/citiesRandom20b"
    $test_func $concept "$attr" $files

    concept="country"
    files="data/countries20a"
    attr="population"
    $test_func $concept "$attr" $files
    attr="population growth"
    $test_func $concept "$attr" $files
    attr="size|area"
    $test_func $concept "$attr" $files
}

test_suite
