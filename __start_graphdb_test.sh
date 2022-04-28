cd redis-query-rust
# cargo build --example rx_query --features experimental-api 
cd ../src
make
make graphdb.so
cd ../..
# sudo pkill redis-server
# src/redis-server --bind 0.0.0.0 --dbfilename data.rdb &
# sleep 5
../src/redis-cli module load src/rxIndexer.so 192.168.1.182 6379
../src/redis-cli module load src/rxQuery.so  192.168.1.182 6379 "&"
# ../src/redis-cli module load  /home/pi/redis/redis-6.0.10/redis-query-rust/target/debug/examples/librx_query.so  192.168.1.182 6379 "&"

../src/redis-cli module load src/rxGraphdb.so 
../src/redis-cli module list
# ../src/redis-cli flushdb
../src/redis-cli -h 192.168.1.182 -p 6379  module load redis-query-rust/target/debug/examples/librx_fetch.so a b c
../src/redis-cli -h 192.168.1.182 -p 6379  module list
# ../src/redis-cli -h 192.168.1.182 -p 6379 flushdb
../src/redis-cli g.set file "./test_data/bi10-en.graph.json"
# ../src/redis-cli g.set file "./test_data/nwt-en.graph.json"
# # wait 180
# # ../src/redis-cli g.set file "./test_data/nwt-de.graph.json"
# # wait 180
# # ../src/redis-cli g.set file "./test_data/nwt-it.graph.json"
# # wait 180
# # ../src/redis-cli g.set file "./test_data/nwt-nl.graph.json"
# # wait 180
# echo "bible/en/nwtsty"
# ../src/redis-cli hgetall "bible/en/nwtsty"
# ../src/redis-cli smembers "^bible/en/nwtsty" 
# echo "bible/en/nwtsty/Genesis"
# ../src/redis-cli hgetall "bible/en/nwtsty/Genesis"
# ../src/redis-cli smembers "^bible/en/nwtsty/Genesis" 
# echo "book:bible/en/nwtsty:bible/en/nwtsty/Genesis"
# ../src/redis-cli hgetall "book:bible/en/nwtsty:bible/en/nwtsty/Genesis"
# ../src/redis-cli smembers "^book:bible/en/nwtsty:bible/en/nwtsty/Genesis" 
# echo redis rx.query "g:v().has('name','Genesis')"
# ../src/redis-cli rx.query "g:v().has('name','Genesis')"
# echo redis rx.query "g:v().has('name','Genesis').out('chapter')"
# ../src/redis-cli rx.query "g:v().has('name','Genesis').out('chapter')"
# echo rx.query jehovah | geovah | jehova
# ../src/redis-cli rx.query jehovah | geovah | jehova
# echo


# echo "^bible/en/nwtsty";../src/redis-cli smembers "^bible/en/nwtsty" 0 100
# echo "^bible/en/nwtsty/Psalm";../src/redis-cli smembers "^bible/en/nwtsty/Psalm" 0 100
# echo "^book:bible/en/nwtsty:bible/en/nwtsty/Psalm";../src/redis-cli smembers "^book:bible/en/nwtsty:bible/en/nwtsty/Psalm" 0 100
# echo "^chapter:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117";../src/redis-cli smembers "^chapter:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117" 0 100
# echo "^bible/en/nwtsty/Psalm/117/2649";../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2649" 0 100
# echo "^bible/en/nwtsty/Psalm/117/2649/v19-117-1-2;../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2649/v19-117-1-2" 0 100
# echo "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2649/v19-117-1-2";../src/redis-cli smembers "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2649/v19-117-1-2" 0 100

# echo "^bible/en/nwtsty/Psalm/117/2650;../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2650" 0 100
# echo "^bible/en/nwtsty/Psalm/117/2650/v19-117-2-2";../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2650/v19-117-2-2" 0 100
# echo "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2650/v19-117-2-2";../src/redis-cli smembers "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2650/v19-117-2-2" 0 100

# echo "^bible/en/nwtsty/Psalm/117/2651";../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2651" 0 100
# echo "^bible/en/nwtsty/Psalm/117/2651/v19-117-2-3";../src/redis-cli smembers "^bible/en/nwtsty/Psalm/117/2651/v19-117-2-3" 0 100
# echo "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2651/v19-117-2-3";../src/redis-cli smembers "^verse:bible/en/nwtsty/Psalm:bible/en/nwtsty/Psalm/117/2651/v19-117-2-3" 0 100


# ../src/redis-cli rx.query "g:v().has('Name','Genesis').out('chapter')"


# ../src/redis-cli rxquery jehovah as 'J-KJV'

../src/redis-cli
