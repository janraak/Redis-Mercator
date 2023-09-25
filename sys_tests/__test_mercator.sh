pwd
# sudo pkill gdb
# sudo pkill python3
# sudo pkill python3
# sudo pkill redis-server
# sudo pkill redis-cli
rm ./sys_tests/runlogs/*.log

export SAVED=$PWD
echo $SAVED
export LD_LIBRARY_PATH=$SAVED/src:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH
echo $LD_LIBRARY_PATH
cd ..

if [[ $SAVED == *7.* ]] # * is used for pattern matching
then
  echo "Not Redis 7"; 
  export ENABLE_MODULE_LOAD="--enable-module-command yes";
else
  echo "Not Redis 7"; 
fi
export ENABLE_MODULE_LOAD="--enable-module-command yes";
echo 'ENABLE_MODULE_LOAD = $ENABLE_MODULE_LOAD'
# valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/runlogs/redis.log" --dbfilename data.rdb &
echo start controller with valgrind
src/redis-server  --bind 0.0.0.0 --port 6380  --dbfilename 6380.rdb --logfile "$SAVED/sys_tests/run_logs/redis-6380.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo start data node with valgrind
# src/redis-server  --bind 0.0.0.0 --port 6381  --dbfilename 6381.rdb --logfile "$SAVED/sys_tests/run_logs/redis-6381.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo start index node with valgrind
# src/redis-server  --bind 0.0.0.0 --port 6382  --dbfilename 6382.rdb --logfile "$SAVED/sys_tests/run_logs/redis-6382.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo $SAVED
cd $SAVED
pwd
cd sys_tests
python3 _runner.py runs 1  >run_logs/_sys_tests.log   </dev/null
