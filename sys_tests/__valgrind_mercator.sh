rm ./sys_tests/run_logs/_*.log
bash sys_tests/__valgrind_test_mercator.sh
# python3 sys_tests/_runner.py runs 1 testset _DEV 
make DEV
#  >sys_tests/run_logs/_sys_tests.log 
# python3 sys_tests/_runner.py >sys_tests/run_logs/_sys_tests.log testset valgrind
bash sys_tests/__valgrind_test_mercator_stop.sh
# cd sys_tests
# python3 ___cleanup_valgrind_logs.py