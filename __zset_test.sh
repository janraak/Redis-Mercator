# cd src/modules
# make
# cd ../..
src/redis-cli module unload rxfetch
src/redis-cli module load src/modules/rxFetch.so
src/redis-cli flushall

src/redis-cli rxadd nl s kleur rood 0.33
src/redis-cli rxadd nl s kleur wit 0.33
src/redis-cli rxadd nl s kleur blauw 0.33
src/redis-cli rxadd nl s banen horizontaal 1.0

src/redis-cli rxadd de s kleur rood 0.33
src/redis-cli rxadd de s kleur geel 0.33
src/redis-cli rxadd de s kleur zwart 0.33
src/redis-cli rxadd de s banen horizontaal 1.0

src/redis-cli rxadd be s kleur rood 0.33
src/redis-cli rxadd be s kleur geel 0.33
src/redis-cli rxadd be s kleur zwart 0.33
src/redis-cli rxadd be s banen vertikaal 1.0

src/redis-cli rxadd fr s kleur rood 0.33
src/redis-cli rxadd fr s kleur wit 0.33
src/redis-cli rxadd fr s kleur blauw 0.33
src/redis-cli rxadd fr s banen vertikaal 1.0

src/redis-cli rxadd es s kleur rood 0.66
src/redis-cli rxadd es s kleur geel 0.33
src/redis-cli rxadd es s banen horizontaal 1.0


src/redis-cli rxadd se s kleur blauw 0.66
src/redis-cli rxadd se s kleur geel 0.33
src/redis-cli rxadd se s banen gekruist 1.0

src/redis-cli rxadd ch s kleur rood 0.66
src/redis-cli rxadd ch s kleur wit 0.33
src/redis-cli rxadd ch s banen gekruist 1.0

src/redis-cli rxadd dk s kleur rood 0.66
src/redis-cli rxadd dk s kleur wit 0.33
src/redis-cli rxadd dk s banen gekruist 1.0

src/redis-cli rxadd it s kleur rood 0.33
src/redis-cli rxadd it s kleur wit 0.33
src/redis-cli rxadd it s kleur groen 0.33
src/redis-cli rxadd it s banen horizontaal 1.0

src/redis-cli rxadd hu s kleur rood 0.33
src/redis-cli rxadd hu s kleur wit 0.33
src/redis-cli rxadd hu s kleur groen 0.33
src/redis-cli rxadd hu s banen vertikaal 1.0


src/redis-cli rxadd gr s kleur blauw 0.55
src/redis-cli rxadd gr s kleur wit 0.44
src/redis-cli rxadd gr s banen horizontaal 1.0
src/redis-cli rxadd gr s banen kruis 1.0

src/redis-cli zrange _zx_:wit:kleur 0 3 withscores
src/redis-cli zrevrange _zx_:wit:kleur 0 3 withscores

src/redis-cli zinterstore roodwit 2 _zx_:wit:kleur _zx_:rood:kleur aggregate max
src/redis-cli zrange roodwit 0 3 withscores
src/redis-cli zrevrange roodwit 0 3 withscores

src/redis-cli rxfetch ge
src/redis-cli zcount _geel_gekruist 0 10
src/redis-cli zrevrange _geel_gekruist 0 7
src/redis-cli 
