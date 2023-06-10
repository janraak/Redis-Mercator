../src/redis-cli -p 6380 --user admin shutdown nosave
../src/redis-cli -p 6381 shutdown nosave
../src/redis-cli -p 6382 shutdown nosave

../src/redis-cli -p 6380 --user admin --pass admin shutdown nosave
../src/redis-cli -p 6381 --user admin --pass admin shutdown nosave
../src/redis-cli -p 6382 --user admin --pass admin shutdown nosave
