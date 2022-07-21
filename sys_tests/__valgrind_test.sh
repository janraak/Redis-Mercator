pwd
../src/redis-cli shutdown nosave
sudo pkill redis-server
rm ./sys_tests/_valgrind-redis.log

export SAVED=$PWD
echo $SAVED
cd /home/pi/redis/redis-6.0.10/
# valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/_valgrind-redis.log" --dbfilename data.rdb &
valgrind  --leak-check=yes  --log-file="$SAVED/sys_tests/_valgrind.log"  /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --logfile "$SAVED/sys_tests/_valgrind-redis.log" --maxmemory 6G &
cd $SAVED
sleep 10
../src/redis-cli monitor >$SAVED/sys_tests/_monitor.log &
pwd
python3 sys_tests/gremlin_001.py 
../src/redis-cli  module list
../src/redis-cli  flushall
../src/redis-cli  g.set FILE /home/pi/redis/redis-6.0.10/extensions/graph/nwtsty-en.graph.json
../src/redis-cli rxindex wait
../src/redis-cli  rxreindex

# src/redis-cli  rule.apply 'United States of America'
# src/redis-cli  rule.apply 'United States of America'
# src/redis-cli test "g.as($$ISCOUNTRY)"
# src/redis-cli test "g.as($$ISCOUNTRY)"
../src/redis-cli rxindex wait
../src/redis-cli  module unload rxIndexer
../src/redis-cli  module unload rxFetch
../src/redis-cli  module unload rxRule
../src/redis-cli  module unload RXQUERY
../src/redis-cli  module unload graphdb
../src/redis-cli shutdown nosave
