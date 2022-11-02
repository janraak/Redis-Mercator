# Test set: Load string keys with Text
flags = {
"NL": "country: The Netherlands; pattern: stripes; orientation: horizontal; colors: red,white,blue; currency: euro; languages: dutch; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/netherlands.png />", 
"IRL":  "country: Ireland; pattern: stripes; orientation: vertical; colors; green,white,orange; currency: euro; languages: english gaelic; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/ireland.png />",
"BE":  "country: Belgium; pattern: stripes; orientation: vertical; colors; black,yellow,red; currency: euro; languages: french dutch; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/belgium.png />",
"IT":  "country: Italy; pattern: stripes; orientation: vertical; colors; green,white,red; currency: euro; languages: italian; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/italy.png />",
"FR":  "country: France;  pattern: stripes; orientation: vertical; colors; red white blue; currency: euro; languages: french; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/france.png />",
"UKS": { "country": "Scotland", "pattern": "solid, icon, 50/50, diagonal cross", "colors": "blue",  "icon-color":"white", "icon-position": "center", "currency": "Brittish Pound", "languages": "english", "emblem": "<img src=https://upload.wikimedia.org/wikipedia/commons/thumb/1/10/Flag_of_Scotland.svg/267px-Flag_of_Scotland.svg.png />"},
"UKNI": { "country": "Northern Ireland", "pattern": "solid icon,50/50, diagonal cross", "colors":"white",  icon-color "red" icon-position "center" currency "Brittish Pound" languages "english" emblem "<img src=https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Saint_Patrick%27s_Saltire.svg/400px-Saint_Patrick%27s_Saltire.svg.png />"},
"UKW": { "country": Wales", "pattern": "stripes", " orientation": "horizontal colors "white green" icon dragon icon-color red icon-position center currency "Brittish Pound" languages "english gaelic" emblem  "<img src=https://en.wikipedia.org/wiki/Flag_of_Wales#/media/File:Flag_of_Wales_(1959%E2%80%93present).svg"
"UKE": { "country": England", "pattern": "solid icon '50/50 cross' colors white  icon-color 'red' icon-position "center" currency "Brittish Pound" languages "english" emblem "<img src=https://upload.wikimedia.org/wikipedia/en/thumb/b/be/Flag_of_England.svg/250px-Flag_of_England.svg.png />"
"GB": { "country": 'Great Britain'", "pattern": "solid icon '50/50 double cross diagonal' colors blue  icon-color red icon-position "center" currency "Brittish Pound" languages "english" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/united_kingdom_of_great_britain_and_northern_ireland.png?itok=ybmcXnYr />"
"IS": { "country": Iceland", "pattern": "solid icon '40/60 cross' colors blue  icon-color 'red over white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/iceland.png />"
"SE": { "country": Sweden", "pattern": "solid icon '40/60 cross' colors blue  icon-color 'yellow' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/sweden.png />"
"N": { "country": Norway", "pattern": "solid icon '40/60 cross' colors red  icon-color 'blue over white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/norway.png />"
"DK": { "country": Denmark", "pattern": "solid icon '40/60 cross' colors red  icon-color 'white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/denmark.png />"
"FI": { "country": Finland", "pattern": "solid icon '40/60 cross' colors white  icon-color 'blue' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/finland.png />"
"CH": { "country": 'Switzerland'", "pattern": "solid icon '25/25 cross' colors red  icon-color white icon-position "center" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/switzerland.png?itok=3ohvckBn />"
"AU",  {"country: Austria; pattern: stripes; orientation: horizontal; colors; red,white,red; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/austria.png?itok=VU8Gynxk />"  }'
"CW",  {"country: Czechoslovakia; pattern: stripes; orientation: horizontal; colors; white,red; icon: right triangle; icon-color: blue; icon-position: left; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/c/cb/Flag_of_the_Czech_Republic.svg/250px-Flag_of_the_Czech_Republic.svg.png />" }'
"CZ",  {"country: Czech Republic; pattern: stripes; orientation: horizontal; colors; white,red; icon: right triangle; icon-color: blue; icon-position: left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/czechia_czech_republic.jpg />" }'
"SK",  {"country: Slovakia; pattern: stripes; orientation: horizontal; colors; white,red,blue; icon: shield patriarchal cross; icon-color: red white blue; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/slovakia.png?itok=PXaX3fol />"  }'
"SI",  {"country: Slovenia; pattern: stripes; orientation: horizontal; colors; white,blue,red; icon: shield mountain peaks; icon-color: white ; icon-position: top left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/slovenia.png />"  }'
"HU",  {"country: Hungary; pattern: stripes; orientation: horizontal; colors; red,white,green; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/hungary.png />" }'

"LU",  {"country: Luxembourg; pattern: stripes; orientation: horizontal; colors; red,white,azure blue; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/luxembourg.png />" }'
"DE",  {"country: Germany; pattern: stripes; orientation: horizontal; colors; black,red,yellow; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/germany.png />" }'
"RU",  {"country: Russia; pattern: stripes; orientation: horizontal; colors; white,blue,red; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/russian_federation.png />" }'
"ES",  {"country: Spain; pattern: stripes; orientation: horizontal; colors; red 25%,yellow 50%,red 25%; icon: shield; icon-color: red; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/spain.png />" }'
"PT",  {"country: Portugal; pattern: stripes; orientation: horizontal; colors; green 40%,red 60%; icon: shield; icon-color: yellow; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/portugal.png />" }'
"RU",  {"country: USSR; pattern: solid; icon: hammer and sickel; colors; red; icon-color: yellow; icon-position: top left; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/a/a9/Flag_of_the_Soviet_Union.svg/510px-Flag_of_the_Soviet_Union.svg.png />" }'
"AD",  {"country: Andorra; pattern: stripes; orientation: vertical; colors; blue,yellow,red; icon: shield; icon-color: red; icon-position: center; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/andorra.png?itok=_bv27wFU />" }'
"SM",  {"country: San Marino; pattern: stripes; orientation: horizontal; colors; white, azure blue; icon: shield; icon-color: green gold; icon-position: center; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/san_marino.png />" }'
"MC",  {"country: Monaco; pattern: stripes; orientation: horizontal; colors; red,white; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/monaco.png />" }'
"MT",  {"country: Malta; pattern: stripes; orientation: horizontal; colors; white,red; icon: cross; icon-color: silver gray; icon-position: top left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/malta.png?itok=cdl4QTx2 />" }'
"VC",  {"country: Vatican City; pattern: stripes; orientation: vertical; colors; yellow,white; icon: crown; icon-color: yellow; icon-position: center right; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Flag_of_the_Vatican_City.svg/250px-Flag_of_the_Vatican_City.svg.png />" }'
"GI",  {"country: Gibraltar; pattern: stripes; orientation: horizontal; colors; 80% white, 20% red; icon: castle; icon-color: red; icon-position: center; emblem: <img src=https://en.wikipedia.org/wiki/File:Flag_of_Gibraltar.svg />" }'
"IM",  {"country: Isle of Man; pattern: solid; colors":  red; icon: tri-legs; icon-color: white yellow; icon-position: center right; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/b/bc/Flag_of_the_Isle_of_Man.svg/260px-Flag_of_the_Isle_of_Man.svg.png />" }'
"LI",  {"country: Liechtenstein; pattern: stripes; orientation: horizontal; colors; blue,red; icon: crown; icon-color: yellow; icon-position: top left; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/4/47/Flag_of_Liechtenstein.svg/250px-Flag_of_Liechtenstein.svg.png />" }'

"NL",  {"country: The Netherlands; archetype: kingdom; pattern: stripes; orientation: horizontal; colors; red,white,blue; currency: euro; languages: dutch; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/netherlands.png />" }'
"IRL",  {"country: Ireland; archetype: republic; pattern: stripes; orientation: vertical; colors; green,white,orange; currency: euro; languages: english gaelic; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/ireland.png />" }'
"BE",  {"country: Belgium; archetype: kingdom; pattern: stripes; orientation: vertical; colors; black,yellow,red",   "currency: euro; languages: french dutch; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/belgium.png />" }'
"IT",  {"country: Italy; archetype: republic",    "pattern: stripes; orientation: vertical; colors; green,white,red",    "currency: euro; languages: italian; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/italy.png />" }'
"FR",  {"country: France; archetype: republic",   "pattern: stripes; orientation: vertical; colors; red white blue",     "currency: euro; languages: french; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/france.png />" }'
"LU",  {"country: Luxembourg; archetype: aristocracy; pattern: stripes; orientation: horizontal; colors; red,white,azure blue; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/luxembourg.png />" }'
"DE",  {"country: Germany; archetype: republic; pattern: stripes; orientation: horizontal; colors; black,red,yellow; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/germany.png />" }'
"RU",  {"country: Russia; archetype: republic; pattern: stripes; orientation: horizontal; colors; white,blue,red; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/russian_federation.png />" }'
"ES",  {"country: Spain; archetype: kingdom; pattern: stripes; orientation: horizontal; colors; red 25%,yellow 50%,red 25%; icon: shield; icon-color: red; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/spain.png />" }'
"PT",  {"country: Portugal; archetype: republic; pattern: stripes; orientation: horizontal; colors; green 40%,red 60%; icon: shield; icon-color: yellow; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/portugal.png />" }'
"RU",  {"country: USSR; archetype: republic; pattern: solid; icon: hammer and sickel; colors; red; icon-color: yellow; icon-position: top left; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/a/a9/Flag_of_the_Soviet_Union.svg/510px-Flag_of_the_Soviet_Union.svg.png />" }'
echo Iceland
"IS": { "country": Iceland archetype republic", "pattern": "solid icon '40/60 cross' colors blue  icon-color 'red over white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/iceland.png />"
"SE": { "country": Sweden archetype kingdom", "pattern": "solid icon '40/60 cross' colors blue  icon-color 'yellow' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/sweden.png />"
"N": { "country": Norway archetype kingdom", "pattern": "solid icon '40/60 cross' colors red  icon-color 'blue over white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/norway.png />"
"DK": { "country": Denmark archetype kingdom", "pattern": "solid icon '40/60 cross' colors red  icon-color 'white' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/denmark.png />"
"FI": { "country": Finland archetype republic", "pattern": "solid icon '40/60 cross' colors white  icon-color 'blue' icon-position "center left" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/finland.png />"
"CH": { "country": 'Switzerland' archetype republic", "pattern": "solid icon '25/25 cross' colors red  icon-color white icon-position "center" emblem "<img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/switzerland.png?itok=3ohvckBn />"
"AU",  {"country: Austria; pattern: stripes; orientation: horizontal; colors; red,white,red; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/austria.png?itok=VU8Gynxk />"  }'
"CW",  {"country: Czechoslovakia; pattern: stripes; orientation: horizontal; colors; white,red; icon: right triangle; icon-color: blue; icon-position: left; emblem: <img src=https://upload.wikimedia.org/wikipedia/commons/thumb/c/cb/Flag_of_the_Czech_Republic.svg/250px-Flag_of_the_Czech_Republic.svg.png />" }'
"CZ",  {"country: Czech Republic; pattern: stripes; orientation: horizontal; colors; white,red; icon: right triangle; icon-color: blue; icon-position: left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/czechia_czech_republic.jpg />" }'
"SK",  {"country: Slovakia; pattern: stripes; orientation: horizontal; colors; white,red,blue; icon: shield patriarchal cross; icon-color: red white blue; icon-position: center left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/slovakia.png?itok=PXaX3fol />"  }'
"SI",  {"country: Slovenia; pattern: stripes; orientation: horizontal; colors; white,blue,red; icon: shield mountain peaks; icon-color: white ; icon-position: top left; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/slovenia.png />"  }'
"HU",  {"country: Hungary; pattern: stripes; orientation: horizontal; colors; red,white,green; emblem: <img src=https://en.unesco.org/creativity/sites/creativity/files/styles/medium_500px/public/country/flags/hungary.png />" }'


"US": { "country": 'United States of America' archetype republic", "pattern": "'horizontal stripes' icon '40/50 blue area 50 stars' colors 'red white blue'  icon-color 'white stars on blue' icon-position 'top left'  languages 'english spanish' emblem '<img src=https://www.nationsonline.org/flags_big/United_States_lgflag.gif />'
"CA": { "country": Canada archetype republic", "pattern": "'horizontal stripes' icon '60/100 red maple leaf on white area' colors 'red white'  icon-color 'red' icon-position "center" languages 'english french' emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/canada.png />'
"MX": { "country": Mexico archetype republic", "pattern": "'vertical stripes' icon 'brown eagle' colors 'green white red'  icon-color 'brown' icon-position "center" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/mexico.png />'
"GT": { "country": Guatemala archetype republic", "pattern": "stripes icon 'scroll bird weapons' colors 'light blue white light blue'  icon-color 'white' icon-position "center" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/guatemala.png />'
"BZ": { "country": Belize archetype republic", "pattern": "'horizontal stripes' icon 'natives' colors '10% red, 80%blue, 10%red'  icon-color 'white' icon-position "center" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/belize.png />'
"HN": { "country": Honduras archetype republic", "pattern": "'horizontal stripes' icon '5 stars' colors 'azure blue white azure blue'  icon-color 'azure blue' icon-position "center" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/guatemala.png />'
"SV": { "country": 'El Salvador' archetype republic", "pattern": "'horizontal stripes' icon 'banners' colors 'azure blue white azure blue'  icon-color 'azure blue' icon-position "center" languages  spanish emblem '<img https://en.unesco.org/sites/default/files/styles/img_201x130/public/flags/SLV.jpg />'
"NI": { "country": 'Nicaragua' archetype republic", "pattern": "'horizontal stripes' icon 'triangle' colors ' blue white  blue'  icon-color 'green' icon-position "center" languages  spanish emblem '<img https://en.unesco.org/sites/default/files/styles/img_201x130/public/flags/SLV.jpg />'
"NI": { "country": 'Nicaragua' archetype republic", "pattern": "'horizontal stripes' icon 'scroll bird weapons' colors 'light blue white light blue'  icon-color 'white' icon-position "center" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/nicaragua.png />'
"CR": { "country": 'Costa Rica' archetype republic", "pattern": "'horizontal stripes' icon 'mountains' colors 'blue white red white blue'  icon-color 'white' icon-position "center" languages  spanish emblem '<img src=https://www.nationsonline.org/flags_big/Costa_Rica_lgflag.gif />'
"PA": { "country": Panama archetype republic", "pattern": "'horizontal rectangles' icon 'stars' colors 'white red white blue'  icon-color 'blue red' icon-position "centered on diaginally opposing white rectangles" languages  spanish emblem '<img src=https://en.unesco.org/creativity/sites/creativity/files/country/flags/panama.png />'



src/redis-cli module load /home/pi/redis/redis-6.0.10/redis-query-rust/target/debug/examples/librx_query.so  192.168.1.28 6379
src/redis-cli module list
src/redis-cli 


src/redis-cli DEL  "CA" "SE" "SV" "LI" "HN" "UKW" "MC" "CW" "IS" "DK" "AU" "DE" "NL" "CZ" "AD" "UKE" "GB" "HU" "BE" "SI" "SM" "GI" "IRL" "LU" "UKS" "N" "ES" "BZ" "GT" "NI" "IT" "UKNI" "MX" "VC" "CR" "RU" "CH" "MT" "US" "FI" "PT" "FR" "SK" "IM" "PA"
src/redis-cli module load iam/target/debug/libiam.so
src/redis-cli 
}
expect = [  # /* set 2*/
    {
        "pp: yes",
        "q": ["range; >=; 300"],
        "k": [
            "fisker/ocean/extreme",
            "fisker/ocean/sport",
            "fisker/ocean/ultra",
            "ford/mustang-e/cr1",
            "ford/mustang-e/p",
            "tesla/3/p",
            "tesla/3/lr",
            "tesla/s",
            "tesla/x",
            "tesla/y/lr",
            "tesla/y/p",
        ],
    },
    {
        "pp: yes",
        "q": ["price; <=; 60000"],
        "k": [
            "fisker/ocean/sport",
            "fisker/ocean/ultra",
            "ford/mustang-e/e",
            "ford/mustang-e/cr1",
            "ford/mustang-e/p",
            "tesla/3",
            "tesla/3/lr",
            "tesla/3/p",
            "volvo/C40",
            "volvo/XC40",
        ],
    },
    {
        "pp: yes",
        "q": ["(; range; >=; 300; ); &&; (; price; <=; 60000; )"],
        "k": [
            "fisker/ocean/ultra",
            "ford/mustang-e/p",
            "tesla/3/lr",
        ],
    },
    {
        "pp: yes",
        "q": ["range; >=; 300; &; price; <=; 60000"],
        "k": [
            "fisker/ocean/ultra",
            "ford/mustang-e/p",
            "tesla/3/lr",
        ],
    },
]
