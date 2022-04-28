
src/redis-cli shutdown nosave
sudo pkill redis-server
rm _valgrind-redis.log
cd src/modules
pwd
make clean
make
make graphdb.so
make rxRule.so
cd ../..
valgrind  --leak-check=yes  --log-file="./sys_tests/_valgrind.log"  src/redis-server  --bind 0.0.0.0 --logfile "./sys_tests/_valgrind-redis.log"  &
sleep 5
src/redis-cli monitor >./sys_tests/_monitor.log &
python3 sys_tests/gremlin_001.py 
src/redis-cli  rule.apply 'United States of America'
src/redis-cli test "g.as($$ISCOUNTRY)"
src/redis-cli test "g.as($$ISCOUNTRY)"
# src/redis-cli shutdown nosave
