# search space
echo "byRank"
eval-scripts/byRank.sh 100.db 10 > result-data/byRank.csv

echo "Performance"
sqlite3 100.db < eval-scripts/perf-E.sql > result-data/perf-e.csv
sqlite3 100.db < eval-scripts/perf-nC.sql > result-data/perf-nC.csv
sqlite3 100.db < eval-scripts/perf-s.sql > result-data/perf-s.csv

echo "Coverage"
sqlite3 100.db < eval-scripts/coverageByDomain.sql | sed 's/"//g' > result-data/coverageByDomain.csv

echo "Precision"
sqlite3 test.db < eval-scripts/byDomainCorrRelaxedTopOrTail.sql | sed 's/"//g' > result-data/byDomainCorrRelaxedTopOrTail.csv
sqlite3 test.db < eval-scripts/precImprovFrom1.sql| sed 's/"//g' > result-data/improvFrom1.csv

echo "Tags"
sqlite3 test.db < eval-scripts/corr-tags-new-ths.sql | sed 's/"//g' > result-data/newCorrThs.csv
sqlite3 test.db < eval-scripts/corr-tags-new-ths-byDomain.sql | sed 's/"//g' > result-data/newCorrThsByDomain.csv

