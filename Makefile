# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

test:
	# cd src && $(MAKE) $@
	python3 sys_tests/gremlin_001.py 

valgrind:
	# cd src && $(MAKE) $@
	sys_tests/__valgrind_test.sh

.PHONY: install
