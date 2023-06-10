pwd
# ../src/redis-cli shutdown nosave
# sudo pkill gdb
# sudo pkill redis-server
rm ./sys_tests/run_logs/_*.log

export SAVED=$PWD
echo $SAVED
export LD_LIBRARY_PATH=$SAVED/src:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH
echo $LD_LIBRARY_PATH
cd ..

if [[ $SAVED == *7.* ]] # * is used for pattern matching
then
  export ENABLE_MODULE_LOAD="--enable-module-command yes";
else
  echo "Not Redis 7"; 
fi
echo 'ENABLE_MODULE_LOAD = $ENABLE_MODULE_LOAD'
# valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/run_logs/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/run_logs/_valgrind-redis.log" --dbfilename data.rdb &
echo start controller with valgrind
valgrind  -s --leak-check=full --show-leak-kinds=all  --log-file="$SAVED/sys_tests/run_logs/_valgrind-6380.log"  src/redis-server  --bind 0.0.0.0 --port 6380 --dir ./data --dbfilename 6380.rdb --logfile "$SAVED/sys_tests/run_logs/_valgrind-redis-6380.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo start data node with valgrind
valgrind  -s --leak-check=full --show-leak-kinds=all  --verbose  --show-reachable=yes  --track-origins=yes --log-file="$SAVED/sys_tests/run_logs/_valgrind-6381.log"  src/redis-server  --bind 0.0.0.0 --port 6381 --dir ./data --dbfilename 6381.rdb --logfile "$SAVED/sys_tests/run_logs/_valgrind-redis-6381.log" --maxmemory 4G --protected-mode no $ENABLE_MODULE_LOAD &
echo start index node with valgrind
valgrind -s --leak-check=full --show-leak-kinds=all  --verbose   --show-reachable=yes  --track-origins=yes --log-file="$SAVED/sys_tests/run_logs/_valgrind-6382.log"  src/redis-server  --bind 0.0.0.0 --port 6382 --dir ./data --dbfilename 6382.rdb --logfile "$SAVED/sys_tests/run_logs/_valgrind-redis-6382.log" --maxmemory 4G --protected-mode no $ENABLE_MODULE_LOAD &
echo $SAVED
cd $SAVED
sleep 10
../src/redis-cli -p 6380 monitor >$SAVED/sys_tests/run_logs/_monitor-6380.log &
../src/redis-cli -p 6381 monitor >$SAVED/sys_tests/run_logs/_monitor-6381.log &
../src/redis-cli -p 6382 monitor >$SAVED/sys_tests/run_logs/_monitor-6382.log &
pwd
