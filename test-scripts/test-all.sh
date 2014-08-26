# main test (2nd parameter is number of candidates to retrieve from index)
bash test-scripts/test-allInOne.sh 100.db 100 AllInOneTestInverseSim

# main test (2nd parameter is scale factor, 3rd is scale factor)
bash test-scripts/test-correctness.sh test.db 100 10
# import judgements into correctness test database
sqlite3 answers.db .dump > dump
sqlite3 test.db < dump
