#!/bin/bash

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

INDEX_URL="http://141.76.47.133:8765"
USE_REA2="-2"
# USE_REA2=""

run_correctness() {
    for numCov in 5; do
        echo "running" $1 $2
        for file in `ls $3*`; do
            echo "    " $file
            scripts/run.sh rea.test.CorrectnessDrillBeyond -e "$file" -a "$2" -c "$1" -n $numCov -p $USE_LOCAL_INDEX -f "$database_file" -u $num_index_candidates -s $search_space -r "$4" -i $INDEX_URL $USE_REA2
        done;
    done;
}


test_suite() {
    for i in 20a 20b Random20a Random20b; do
        concept="company"
        files="data/companies$i"
        attr="revenues|revenue|sales"
        $test_func $concept "$attr" $files
        attr="revenues growth|revenue growth|sales growth|revenues change|revenues change|sales change"
        $test_func $concept "$attr" $files
        attr="established|founded"
        $test_func $concept "$attr" $files
        attr="employees"
        $test_func $concept "$attr" $files
    done

    # concept="city"
    # files="data/cities20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/cities20b"
    # $test_func $concept "$attr" $files

    # concept="city"
    # files="data/citiesRandom20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/citiesRandom20b"
    # $test_func $concept "$attr" $files

    concept="country"
    files="data/countries20a"
    attr="population"
    $test_func $concept "$attr" $files
    attr="population growth"
    $test_func $concept "$attr" $files
    # attr="size|area"
    # $test_func $concept "$attr" $files
}

test_suite_isNumeric() {
    for i in 20a 20b Random20a Random20b; do
        concept="company"
        files="data/companies$i"
        attr="revenues|revenue|sales"
        $test_func $concept "$attr" $files isNumeric
        attr="revenues growth|revenue growth|sales growth|revenues change|revenues change|sales change"
        $test_func $concept "$attr" $files isNumeric
        attr="established|founded"
        $test_func $concept "$attr" $files isNumeric
        attr="employees"
        $test_func $concept "$attr" $files isNumeric
    done

    # concept="city"
    # files="data/cities20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/cities20b"
    # $test_func $concept "$attr" $files

    # concept="city"
    # files="data/citiesRandom20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/citiesRandom20b"
    # $test_func $concept "$attr" $files

    concept="country"
    files="data/countries20a"
    attr="population"
    $test_func $concept "$attr" $files isNumeric
    attr="population growth"
    $test_func $concept "$attr" $files isNumeric
    # attr="size|area"
    # $test_func $concept "$attr" $files
}

test_suite_numericPredicate() {
    for i in 20a 20b Random20a Random20b; do
        concept="company"
        files="data/companies$i"
        attr="revenues|revenue|sales"
        $test_func $concept "$attr" $files ">100"
        attr="revenues growth|revenue growth|sales growth|revenues change|revenues change|sales change"
        $test_func $concept "$attr" $files ">10.0"
        attr="established|founded"
        $test_func $concept "$attr" $files "<1900"
        attr="employees"
        $test_func $concept "$attr" $files ">50000"
    done

    # concept="city"
    # files="data/cities20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/cities20b"
    # $test_func $concept "$attr" $files

    # concept="city"
    # files="data/citiesRandom20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/citiesRandom20b"
    # $test_func $concept "$attr" $files

    concept="country"
    files="data/countries20a"
    attr="population"
    $test_func $concept "$attr" $files ">100000000"
    attr="population growth"
    $test_func $concept "$attr" $files ">2.0"
    # attr="size|area"
    # $test_func $concept "$attr" $files
}

test_suite_bothPredicates() {
    for i in 20a 20b Random20a Random20b; do
        concept="company"
        files="data/companies$i"
        attr="revenues|revenue|sales"
        $test_func $concept "$attr" $files ">100|isNumeric"
        attr="revenues growth|revenue growth|sales growth|revenues change|revenues change|sales change"
        $test_func $concept "$attr" $files ">10.0|isNumeric"
        attr="established|founded"
        $test_func $concept "$attr" $files "<1900|isNumeric"
        attr="employees"
        $test_func $concept "$attr" $files ">50000|isNumeric"
    done

    # concept="city"
    # files="data/cities20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/cities20b"
    # $test_func $concept "$attr" $files

    # concept="city"
    # files="data/citiesRandom20a"
    # attr="population|inhabitants"
    # $test_func $concept "$attr" $files
    # files="data/citiesRandom20b"
    # $test_func $concept "$attr" $files

    concept="country"
    files="data/countries20a"
    attr="population"
    $test_func $concept "$attr" $files ">100000000|isNumeric"
    attr="population growth"
    $test_func $concept "$attr" $files ">2.0|isNumeric"
    # attr="size|area"
    # $test_func $concept "$attr" $files
}


if [[ $4 == "" ]]; then
    echo "RUNNING WITH NO PRED"
    test_suite
fi

if [[ $4 == "isNumeric" ]]; then
    echo "RUNNING WITH isNumeric"
    test_suite_isNumeric
fi

if [[ $4 == "numericPredicate" ]]; then
    echo "RUNNING WITH A numericPredicate"
    test_suite_numericPredicate
fi

if [[ $4 == "bothPredicates" ]]; then
    echo "RUNNING WITH A numericPredicate AND isNumeric"
    test_suite_bothPredicates
fi

if [[ $4 == "all" ]]; then
    test_suite
    test_suite_isNumeric
    test_suite_numericPredicate
    test_suite_bothPredicates
fi
