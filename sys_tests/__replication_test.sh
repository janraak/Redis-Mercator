pwd
# /home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.0.180 6379 shutdown nosave
# /home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.0.180 6380 shutdown nosave
# /home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.0.182 6379 shutdown nosave
# /home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.0.182 6380 shutdown nosave
# sudo pkill redis-server

export SAVED=$PWD
echo $SAVED
cd /home/pi/redis/redis-6.0.10/
/home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6379 --logfile "/home/pi/redis/redis-6.0.10/extensions/sys_tests/primary-data-redis.log" --maxmemory 1G &
/home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6380 --logfile "/home/pi/redis/redis-6.0.10/extensions/sys_tests/replica-index-redis.log" --maxmemory 1G &

ssh pi@192.168.1.182 /home/pi/redis/redis-6.0.10/src/redis-cli shutdown nosave
ssh pi@192.168.1.182 sudo pkill redis-server
ssh pi@192.168.1.182 /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6379 --logfile "/home/pi/redis/redis-6.0.10/extensions/sys_tests/primary-index-redis.log" --maxmemory 1G &
ssh pi@192.168.1.182 /home/pi/redis/redis-6.0.10/src/redis-server  --bind 0.0.0.0 --port 6380 --logfile "/home/pi/redis/redis-6.0.10/extensions/sys_tests/replica-data-redis.log" --maxmemory 1G &
/home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.1.182 -p 6380 REPLICAOF 192.168.1.180 6379
/home/pi/redis/redis-6.0.10/src/redis-cli -h 192.168.1.180 -p 6380 REPLICAOF 192.168.1.182 6379


