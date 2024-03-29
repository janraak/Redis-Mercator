# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd src && $(MAKE) $@
	make SANITY

install:
	cd src && $(MAKE) $@

test:
	# cd src && $(MAKE) $@
	# ..//src/redis-server --bind 0.0.0.0 --dbfilename data.rdb --maxmemory 6G &
	# python3 sys_tests/rxIndexStore.py 
	# python3 sys_tests/gremlin_001.py 
	sys_tests/__test_mercator.sh

I:
	# cd src && $(MAKE) $@
	# ..//src/redis-server --bind 0.0.0.0 --dbfilename data.rdb --maxmemory 6G &
	python3 sys_tests/index_test_000.py 

S:
	# ..//src/redis-server --bind 0.0.0.0 --dbfilename data.rdb --maxmemory 6G &
	python3 sys_tests/rxIndexStore.py 
	python3 sys_tests/gremlin_001.py 
	# ../src/redis-cli  test.json test1 'j:{"woonplaatsen":["Oosthuizen, NH, NL", "Amsterdam, NH, NL", "Enkhuizen, NH, NL", "Hoogkarspel, NH, NL", "Oosthuizen, NH, NL", "shoreline,wa,usa"]}'
	# ../src/redis-cli  test.json test2 'j:{"naam":"jan raak", "woonplaats":"shoreline,wa,usa"}'
	# ../src/redis-cli  test.json test3 'j:{"naam":"jan raak", "woonplaatsen":["Oosthuizen, NH, NL", "Amsterdam, NH, NL", "Enkhuizen, NH, NL", "Hoogkarspel, NH, NL", "Oosthuizen, NH, NL", "shoreline,wa,usa"]}'
	# ../src/redis-cli  test.json test4 "t:naam: jan raak. geboren: velsen. domicilie: shoreline, wa, usa."
	# ../src/redis-cli  test.json  test5 "t:naam: 'jan raak'. geboren: velsen. domicilie: shoreline, wa, usa."
	# ../src/redis-cli  test.json  test6 "t:Jan woont in Shoreline, Jan is getrouwd met Isabel."
	# ../src/redis-cli  test.json  test7 "t:About this item* Landscape edging kit for creating landscaping borders; great for lawns, flower beds, and pathways*  Flexible and easy to install—in either a straight line or molded to form curves*  Made of high-density polyethylene (HDPE) plastic material; solid black color for easy coordinating*  Combats frost heaves and heavy rain to keep landscaping design in place*  Measures 480 by 5 by 1 inches; weighs 8.5 pounds"
	# ../src/redis-cli  test.json  test8 "t:About this item* Landscape edging kit for creating landscaping borders; great for lawns, flower beds, and pathways*  Flexible and easy to install—in either a straight line or molded to form curves*  Made of high-density polyethylene (HDPE) plastic material; solid black color for easy coordinating*  Combats frost heaves and heavy rain to keep landscaping design in place*  Measurements: 480 by 5 by 1 inches; weight: 8.5 pounds"
	
	../src/redis-cli   rxindex off
	../src/redis-cli   flushall
	../src/redis-cli  g.set2 file "..//extensions/graph/nwtsty-en.graph2.json"
	../src/redis-cli   keys *

T:
	 cd src ; $(MAKE) 
	# ..//src/redis-server --bind 0.0.0.0 --dbfilename data.rdb --maxmemory 6G &
	python3 sys_tests/rxIndexStore.py 
	python3 sys_tests/gremlin_001.py 
	# ../src/redis-cli  test.json test1 'j:{"woonplaatsen":["Oosthuizen, NH, NL", "Amsterdam, NH, NL", "Enkhuizen, NH, NL", "Hoogkarspel, NH, NL", "Oosthuizen, NH, NL", "shoreline,wa,usa"]}'
	# ../src/redis-cli  test.json test2 'j:{"naam":"jan raak", "woonplaats":"shoreline,wa,usa"}'
	# ../src/redis-cli  test.json test3 'j:{"naam":"jan raak", "woonplaatsen":["Oosthuizen, NH, NL", "Amsterdam, NH, NL", "Enkhuizen, NH, NL", "Hoogkarspel, NH, NL", "Oosthuizen, NH, NL", "shoreline,wa,usa"]}'
	# ../src/redis-cli  test.json test4 "t:naam: jan raak. geboren: velsen. domicilie: shoreline, wa, usa."
	# ../src/redis-cli  test.json  test5 "t:naam: 'jan raak'. geboren: velsen. domicilie: shoreline, wa, usa."
	# ../src/redis-cli  test.json  test6 "t:Jan woont in Shoreline, Jan is getrouwd met Isabel."
	# ../src/redis-cli  test.json  test7 "t:About this item* Landscape edging kit for creating landscaping borders; great for lawns, flower beds, and pathways*  Flexible and easy to install—in either a straight line or molded to form curves*  Made of high-density polyethylene (HDPE) plastic material; solid black color for easy coordinating*  Combats frost heaves and heavy rain to keep landscaping design in place*  Measures 480 by 5 by 1 inches; weighs 8.5 pounds"
	# ../src/redis-cli  test.json  test8 "t:About this item* Landscape edging kit for creating landscaping borders; great for lawns, flower beds, and pathways*  Flexible and easy to install—in either a straight line or molded to form curves*  Made of high-density polyethylene (HDPE) plastic material; solid black color for easy coordinating*  Combats frost heaves and heavy rain to keep landscaping design in place*  Measurements: 480 by 5 by 1 inches; weight: 8.5 pounds"
	
	../src/redis-cli   rxindex off
	../src/redis-cli   flushall
	../src/redis-cli  g.set2 file "..//extensions/graph/nwtsty-en.graph copy.json"
	../src/redis-cli   keys *


SANITY:
	cd sys_tests
	python3 sys_tests/_runner.py runs 1000 testset sanity_check


SW:
	cd sys_tests
	python3 sys_tests/_runner.py runs 1 testset sanity_sw

DEV:
	cd sys_tests
	python3 sys_tests/_runner.py runs 1 testset _DEV

SCENARIOS:
	cd sys_tests
	python3 sys_tests/_runner.py runs 1 testset scenarios

V:
	# cd src ; $(MAKE) 
	sys_tests/__valgrind_test.sh

valgrind:
	# cd src && $(MAKE) $@
	sys_tests/__valgrind_test.sh

.PHONY: install
