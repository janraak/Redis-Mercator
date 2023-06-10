rm ./sys_tests/run_logs/_*.log
bash sys_tests/__valgrind_test_mercator.sh
python3 sys_tests/_runner.py >sys_tests/run_logs/_sys_tests.log testset valgrind
bash sys_tests/__valgrind_test_mercator_stop.sh
