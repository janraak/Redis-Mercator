pwd
# ../src/redis-cli shutdown nosave
# sudo pkill gdb
# sudo pkill redis-server
rm ./sys_tests/_*.log

export SAVED=$PWD
echo $SAVED
cd /home/pi/redis/redis-6.0.10/
# valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/_valgrind-redis.log" --dbfilename data.rdb &
valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind-6379.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6379 --dbfilename 6379.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6379.log" --maxmemory 1G &
valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind-6400.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6400 --dbfilename 6400.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6400.log" --maxmemory 1G &
valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind-6401.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6401 --dbfilename 6401.rdb --logfile "$SAVED/sys_tests/_valgrind-redis-6401.log" --maxmemory 1G &
cd $SAVED
sleep 10
../src/redis-cli -p 6379 monitor >$SAVED/sys_tests/_monitor-6379.log &
../src/redis-cli -p 6400 monitor >$SAVED/sys_tests/_monitor-6400.log &
../src/redis-cli -p 6401 monitor >$SAVED/sys_tests/_monitor-6401.log &
pwd
