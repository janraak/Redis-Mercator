pwd
# sudo pkill gdb
# sudo pkill python3
# sudo pkill python3
# sudo pkill redis-server
# sudo pkill redis-cli
rm ./sys_tests/_*.log

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
# valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/_valgrind-redis.log" --dbfilename data.rdb &
echo start controller with valgrind
src/redis-server  --bind 0.0.0.0 --port 6379 --dir ./data --dbfilename 6379.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6379.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo start data node with valgrind
src/redis-server  --bind 0.0.0.0 --port 6400 --dir ./data --dbfilename 6400.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6400.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo start index node with valgrind
src/redis-server  --bind 0.0.0.0 --port 6401 --dir ./data --dbfilename 6401.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6401.log" --maxmemory 1G --protected-mode no $ENABLE_MODULE_LOAD &
echo $SAVED
cd $SAVED
pwd
cd sys_tests
python3 _runner.py >_sys_tests.log 
