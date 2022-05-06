# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

test:
	# cd src && $(MAKE) $@
	# /home/pi/redis/redis-6.0.10/src/redis-server --bind 0.0.0.0 --dbfilename data.rdb --maxmemory 6G &
	python3 sys_tests/rxfetch.py 
	python3 sys_tests/gremlin_001.py 

valgrind:
	# cd src && $(MAKE) $@
	sys_tests/__valgrind_test.sh

.PHONY: install
