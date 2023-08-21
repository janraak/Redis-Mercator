[33mcommit dd3b5336a312e8d53c29ac36147335930a23702f[m[33m ([m[1;36mHEAD -> [m[1;32mmain[m[33m, [m[1;31morigin/main[m[33m, [m[1;31morigin/HEAD[m[33m)[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Aug 21 04:03:47 2023 +0000

    Ignore session storage (not compiling with redis 7.2.0)

[33mcommit fa085fbab174427f1566b0d03c769e0af2cf33ff[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Aug 21 03:55:01 2023 +0000

    Ignore session storage (not compiling with redis 7.2.0)

[33mcommit cb9dcbde02fb8f0d39f89c4124277a2c0162fb9e[m
Author: Jan Raak <jan.raak@live.com>
Date:   Sun Aug 20 20:58:58 2023 +0000

    Mercator Redis Installation

[33mcommit 7307ac654e2fd019d6c40fde654f845488fa9f70[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Aug 14 22:34:01 2023 +0000

    Health Check with Time on each update

[33mcommit a17b4a848be3312617853dd2d794d23f6d2875e6[m
Author: Jan Raak <jan.raak@live.com>
Date:   Fri Jul 21 01:38:01 2023 +0000

    Refactoring: create cluster as async operation
    
    New: cluster and instance health monitor
         instance restart
         cluster and instance key stats
    
    New: Object expression, where step function
    
    Bugs and optimizations

[33mcommit 51401627ddd0794337861ba84d9f8b51cff040aa[m
Author: Jan Raak <jan.raak@live.com>
Date:   Sat Jun 10 01:45:51 2023 +0000

    1) Multi-match minmimize/maximize refactored
    2) System test refactored

[33mcommit 207cda500dd762e1e81298341f237552a34b69f0[m
Author: Jan Raa <jan.raak@live.com>
Date:   Fri Jun 2 11:44:52 2023 -0700

    1) Expression as parameter
    2) Match parameters refactored
    3) Enabled prod and dev environments on same server
    4) Refactoring: many build warnings removed

[33mcommit 90406345f9026dab94282db8f9399b872f9a5b5f[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sun May 14 21:20:56 2023 -0700

    Gremlin: Match stabilized + optimized

[33mcommit 92faacfe72b303ea73672b42e130dea40ec80fda[m
Author: Jan Raak <jan.raak@live.com>
Date:   Tue Apr 11 13:59:12 2023 -0700

    Merge branch 'clientInfo'

[33mcommit 713e97608b32c2d1fa3283016d96d457c89e1658[m[33m ([m[1;31morigin/scoring[m[33m, [m[1;31morigin/merge-indexer-commands[m[33m, [m[1;31morigin/csvload[m[33m)[m
Author: Jan Raa <jan.raak@live.com>
Date:   Fri Jan 20 15:18:16 2023 -0800

    Stabilization, demo indexing and rules scenarios run flawless many times over
    
    Stabilization, demo indexing and rules scenarios run flawless many times over
    
    Stabilization
    
    Stabilization

[33mcommit 02a1cdb46a474899c0086d20571d18fa48ba332f[m
Author: Jan Raa <jan.raak@live.com>
Date:   Fri Jan 20 15:18:16 2023 -0800

    Stabilization, demo indexing and rules scenarios run flawless many times over
    
    Stabilization, demo indexing and rules scenarios run flawless many times over

[33mcommit dd4515fda38fa8a22a97c78f34d1ec2cec6e9d34[m
Author: Jan Raak <jan.raak@live.com>
Date:   Thu Jan 5 10:26:45 2023 -0800

    Stabilization

[33mcommit f88f80ba6a8b833b70331a92a51873993fa6c92e[m
Author: Jan Raak <jan.raak@live.com>
Date:   Thu Dec 29 18:45:51 2022 -0800

    clientinfo and some bug fixes

[33mcommit 22ffd58ce55676430f76eba9f4d2839e1642426d[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Dec 26 14:55:33 2022 -0800

    Merge rxinfo + rxindex commands

[33mcommit 6db50521202677484653b274e8cc354728eda8c0[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Dec 26 13:39:49 2022 -0800

    tabular text file load + score ranked/filtered

[33mcommit 3fa2d55a6da51d2070ca7f498674e66158e9ebff[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Dec 19 13:58:31 2022 -0800

    tabular text file load + score ranked/filtered

[33mcommit f05b3fa594a8d46c41431779acb69e2ccac0b661[m
Author: Jan Raak <jan.raak@live.com>
Date:   Wed Dec 14 16:30:57 2022 -0800

    Enabled query scores and rxquery/rxget updated.

[33mcommit 8ec5a790ee84eda1f7df84af8d2a3c96d5ce4a9f[m
Author: Jan Raak <jan.raak@live.com>
Date:   Tue Dec 13 17:14:59 2022 -0800

    Redis 7.0 validation

[33mcommit 62ada5f8c4cee0157bd06c87b9a6ca900b4d961f[m
Author: Jan Raak <jan.raak@live.com>
Date:   Tue Dec 13 17:14:18 2022 -0800

    Redis 7.0 validation

[33mcommit b87a7db67966f3976e9d89b09bdc222f620c0635[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 10:03:06 2022 -0800

    Fixing 64bit pointer issues
    
    Fixing siginfo_t error
    
    Fixing 64bit pointer issues
    
    Fixing 64bit pointer issues

[33mcommit 1daf3038a604352a0369e353c40762d90f6c7597[m
Merge: 9e0a9e2 36af2da
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 15:06:56 2022 -0800

    Merge branch 'main' of https://github.com/janraak/Redis-Mercator into main

[33mcommit 9e0a9e2871db2daa51d2a317fb7eb6c40c511641[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 10:03:06 2022 -0800

    Fixing 64bit pointer issues

[33mcommit 36af2daabe2e0f195a27bb747fb148a0ec4aa5b5[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 14:23:46 2022 -0800

    Fixing siginfo_t error

[33mcommit 7ad856a51130b73e3620cec27d8cf5e232b3a23d[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 10:03:06 2022 -0800

    Fixing 64bit pointer issues

[33mcommit c18b0316737a3eced7a3b8e1664f0fffa6bb57aa[m
Merge: 7cf59e4 5649e05
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 09:49:43 2022 -0800

    Merge branch 'main' of https://github.com/janraak/Redis-Mercator into main

[33mcommit 7cf59e450205d4c999d8f6900e93658e18e9d287[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 09:45:38 2022 -0800

    Fixing siginfo_t error

[33mcommit 5649e05f1080ae91c173d6fab310a919a4c16444[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sat Dec 10 09:45:38 2022 -0800

    Fixing siginfo_t error

[33mcommit 2dfd136010ff20f736d56d4db00793c1a7dcc916[m[33m ([m[1;31morigin/r7.0[m[33m)[m
Merge: a1277b0 0875428
Author: Jan Raa <jan.raak@live.com>
Date:   Fri Dec 9 19:12:59 2022 -0800

    Merge branch 'r7.0' of https://github.com/janraak/Redis-Mercator into r7.0

[33mcommit a1277b0a22570b19484319d91fc21da649ece0b8[m
Author: Jan Raa <jan.raak@live.com>
Date:   Mon Nov 28 19:33:27 2022 -0800

    Redis 7.0 validation
    
    Redis 7.0 validation
    
    Redis 7.0 validation
    
    Redis 7.0 validation
    
    Redis 7.0 validation

[33mcommit 08754282dd417628738f8bd15dd98e2ed28bb755[m
Author: Jan Raa <jan.raak@live.com>
Date:   Mon Dec 5 16:38:54 2022 -0800

    Redis 7.0 validation

[33mcommit 49972f8577840e0d8eac35a0aee870bdca8f3995[m
Author: Jan Raa <jan.raak@live.com>
Date:   Sun Dec 4 17:43:52 2022 -0800

    Redis 7.0 validation

[33mcommit 55149489f965e8d3e5ffbf729c2d31213d330e64[m
Author: Jan Raa <jan.raak@live.com>
Date:   Tue Nov 29 09:45:15 2022 -0800

    Redis 7.0 validation

[33mcommit 29b657bcc9c7792af63e47af1bc7d60d5e8e8803[m
Author: Jan Raa <jan.raak@live.com>
Date:   Mon Nov 28 19:33:27 2022 -0800

    Redis 7.0 validation

[33mcommit af40a44d2dadac6a1de72783887b6e4fd6344570[m
Author: Jan Raa <jan.raak@live.com>
Date:   Fri Nov 25 15:38:02 2022 -0800

    Redis 7.0 validation
    
    Redis 7.0 validation

[33mcommit a5fb4bcaf317bcfa081d5ca0f1e16e36b38c332d[m
Author: Jan Raa <jan.raak@live.com>
Date:   Wed Nov 23 16:45:09 2022 -0800

    Redis 6.2 Validation
    
    Redis 6.2 Validation
    
    Redis 6.2 Validation
    
    Redis 6.2 Validation
    
    Redis 6.2 Validation
    
    Redis 6.2 Validation

[33mcommit 1093d3ba95c405ea773e5d1bc894fa6b3b9dee7b[m
Author: Jan Raa <jan.raak@live.com>
Date:   Thu Nov 24 11:27:50 2022 -0800

    Redis 6.2 Validation

[33mcommit 7313bf08257a3c02a112d96c580d0eeaee0e61e2[m
Author: Jan Raa <jan.raak@live.com>
Date:   Wed Nov 23 17:01:06 2022 -0800

    Redis 6.2 Validation

[33mcommit 380bcb7809cef1c1854480e07feb0a99b7d247b5[m
Author: Jan Raa <jan.raak@live.com>
Date:   Wed Nov 23 16:55:55 2022 -0800

    Redis 6.2 Validation

[33mcommit 408942998c266e074577ddb46aa2d3ee56f682ea[m
Author: Jan Raa <jan.raak@live.com>
Date:   Wed Nov 23 16:55:36 2022 -0800

    Redis 6.2 Validation

[33mcommit 959db9cbb96510bd982ffbfd39c1a4658f50e21f[m
Author: Jan Raa <jan.raak@live.com>
Date:   Wed Nov 23 16:45:09 2022 -0800

    Redis 6.2 Validation

[33mcommit f386df98f0e791ddb156a1152df12bb61d30ae18[m
Merge: ab7b887 34c161d
Author: Jan Raa <jan.raak@live.com>
Date:   Tue Nov 22 12:21:31 2022 -0800

    Merge branch 'main' of https://github.com/janraak/Redis-Mercator into main

[33mcommit ab7b88732e2905f5ae6a3cc8aca4a80a5ac99c6f[m
Author: Jan Raa <jan.raak@live.com>
Date:   Tue Nov 1 17:10:44 2022 -0700

    Various bugs fixed, rxMercator added, rxFetch renamed to rxIndexStore
    
    Various bugs fixed
    
    Refactoring: embedded strings, to manage objects
    
    gitignore updated
    
    Merge conflicts resolved
    
    Redis 6.2 fixup

[33mcommit 34c161d8fa298b86dafa175a357a21d8f364679b[m
Author: Jan Raa <jan.raak@live.com>
Date:   Mon Nov 21 17:57:10 2022 -0800

    Merge conflicts resolved

[33mcommit b8ef387e737b8d194c4cf6dc94d4c9d47c1b1805[m
Author: Jan Raa <jan.raak@live.com>
Date:   Tue Nov 1 17:10:44 2022 -0700

    Various bugs fixed, rxMercator added, rxFetch renamed to rxIndexStore
    
    Various bugs fixed
    
    Refactoring: embedded strings, to manage objects
    
    gitignore updated

[33mcommit e952a6050b71893b50040e9d394e178d81c475ee[m
Author: Jan Raa <jan.raak@live.com>
Date:   Tue Nov 1 17:10:44 2022 -0700

    Various bugs fixed, rxMercator added, rxFetch renamed to rxIndexStore

[33mcommit a473add26fd41d6137492adc8628d6a11597f515[m
Author: Jan Raa <jan.raak@live.com>
Date:   Thu Jul 21 11:59:11 2022 -0700

    Refactor g.sey (load json rdl graph) into Redis to be duplexed and non-blocking

[33mcommit 32dd469422587f2b9d4e8e581047471915766511[m
Merge: 477c2eb 000682d
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Jun 27 16:33:13 2022 -0700

    merge conflicts resolved

[33mcommit 477c2eb89c73d27d4ac15c0ca5aed0200685a313[m
Author: Jan Raak <jan.raak@live.com>
Date:   Mon Jun 27 16:11:13 2022 -0700

    rxFetch bug fixes

[33mcommit 000682d4b28d8fbb3f817d0342ff81a96c5538f9[m
Author: Jan Raa <jan.raak@live.com>
Date:   Mon Jun 27 15:58:03 2022 -0700

    Refactored rxIndexer from c to c++

[33mcommit f1c54523314279cf56f8ffefda8a97c83b5feac7[m
Author: Jan Raa <jan.raak@live.com>
Date:   Tue May 10 16:10:18 2022 -0700

    sjiboleth handlers refactored

[33mcommit 77c30c67a6968fc1c1cd73c57a18f6067a6dddf0[m
Merge: ef607e6 d3ee7bb
Author: Jan Raa <jan.raak@live.com>
Date:   Sat May 7 17:22:21 2022 -0700

    Merge branch 'main' of https://github.com/janraak/Redis-Mercator into main

[33mcommit ef607e6ee1281225399ad28949eef91a35a15dc5[m
Author: janraak <jan.raak@live.com>
Date:   Fri May 6 12:27:51 2022 -0700

    Delete test_data directory
    
    Removed test script due to AWS TT V577456845
    
    remove replenished C artefacts
    
    remove binaries

[33mcommit d3ee7bb9a9181f022820804f3f30a3632f2fc442[m
Author: Jan Raa <jan.raak@live.com>
Date:   Fri May 6 13:09:30 2022 -0700

    Removed test script due to AWS TT V577456845

[33mcommit cc63838393a4f514caab936cafe8a14b53bf5c5a[m
Author: janraak <jan.raak@live.com>
Date:   Fri May 6 12:27:51 2022 -0700

    Delete test_data directory

[33mcommit 5343360d243c1d23923ca7c191e1ae1ab77a0789[m
Author: janraak <jan.raak@live.com>
Date:   Wed Apr 27 19:35:57 2022 -0700

    Update README.md + rxSuite:Mercator v0.1
    
    Update README.md
    
    rxSuite: Mercator V0.1

[33mcommit ec00da45d189656b4a6bc22ca4a5423de9d69aa6[m
Author: janraak <jan.raak@live.com>
Date:   Thu Apr 28 11:23:32 2022 -0700

    Update README.md

[33mcommit f45aadac8fb991025ac57b5ded928f2d1a742db1[m
Author: janraak <jan.raak@live.com>
Date:   Wed Apr 27 19:35:57 2022 -0700

    Update README.md

[33mcommit b88ee44782a98ca5b0bf3dfd8292e21c39b53dd9[m
Author: janraak <jan.raak@live.com>
Date:   Tue Apr 26 17:45:24 2022 -0700

    Initial commit
