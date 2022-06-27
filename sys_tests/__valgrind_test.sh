pwd
../src/redis-cli shutdown nosave
sudo pkill redis-server
rm ./sys_tests/_valgrind-redis.log

export SAVED=$PWD
echo $SAVED
cd /home/pi/redis/redis-6.0.10/
valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/_valgrind-redis.log" --dbfilename data.rdb &
cd $SAVED
sleep 10
../src/redis-cli monitor >$SAVED/sys_tests/_monitor.log &
pwd
python3 sys_tests/gremlin_001.py 
../src/redis-cli  rxreindex

# src/redis-cli  rule.apply 'United States of America'
# src/redis-cli  rule.apply 'United States of America'
# src/redis-cli test "g.as($$ISCOUNTRY)"
# src/redis-cli test "g.as($$ISCOUNTRY)"
# src/redis-cli shutdown nosave
