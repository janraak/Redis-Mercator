../src/redis-cli -p 6379 --user admin shutdown nosave
../src/redis-cli -p 6400 shutdown nosave
../src/redis-cli -p 6401 shutdown nosave

../src/redis-cli -p 6379 --user admin --pass admin shutdown nosave
../src/redis-cli -p 6400 --user admin --pass admin shutdown nosave
../src/redis-cli -p 6401 --user admin --pass admin shutdown nosave
