from __future__ import print_function
import redis
import json
import math
import pdb

# Travel options:                   https://www.rome2rio.com/map/
# Distance and CO2 emissions:       https://www.distancesfrom.com/fr/Paris-to-Malta-Route/RouteplannerHistory/17929779.aspx
#
eu_capitals = [
    ("Helsinki", "capital",  { "geography": "continent", "country": "Finland", "eu": "member"}),
    ("Stockholm", "capital",  { "geography": "continent", "country": "Sweden", "eu": "member"}),
    ("Tallinn", "capital",  { "geography": "continent", "country": "Estonia", "eu": "member"}),

    ("Riga", "capital",  { "geography": "continent", "country": "Latvia", "eu": "member"}),
    ("Vilnius", "capital",  { "geography": "continent", "country": "Lithounia", "eu": "member"}),
    ("Warsaw", "capital",  { "geography": "continent", "country": "Poland", "eu": "member"}),
    ("Bucarest", "capital",  { "geography": "continent", "country": "Romania", "eu": "member"}),
    ("Sofia", "capital",  { "geography": "continent", "country": "Bulgaria", "eu": "member"}),
    ("Nicosia", "capital",  { "geography": "island", "country": "Cyprus", "eu": "member"}),
    ("Athens", "capital",  { "geography": "continent", "country": "Greece", "eu": "member"}),
    ("Valletta", "capital",  { "geography": "island", "country": "Malta", "eu": "member"}),
    ("Madrid", "capital",  { "geography": "continent", "country": "Spain", "eu": "member"}),
    ("Lisbon", "capital",  { "geography": "continent", "country": "Portugal", "eu": "member"}),
    ("Dublin", "capital",  { "geography": "island", "country": "Ireland", "eu": "member"}),
    ("Copenhagen", "capital",  { "geography": "continent", "country": "Denmark", "eu": "member"}),

    ("Amsterdam", "capital",  { "geography": "continent", "country": "The Netherlands", "eu": "member"}),
    ("Berlin", "capital",  { "geography": "continent", "country": "Germany", "eu": "member"}),
    ("Bratislava", "capital",  { "geography": "continent", "country": "Slovakia", "eu": "member"}),
    ("Budapest", "capital",  { "geography": "continent", "country": "Hungary", "eu": "member"}),
    ("Rome", "capital",  { "geography": "continent", "country": "Italy", "eu": "member"}),
    ("Paris", "capital",  { "geography": "continent", "country": "France", "eu": "member"}),
    ("Bruxelles", "capital",  { "geography": "continent", "country": "Belgium", "eu": "member"}),

    ("Luxembourg", "capital",  { "geography": "continent", "country": "Luxembourg", "eu": "member"}),
    ("Prague", "capital",  { "geography": "continent", "country": "Czechia", "eu": "member"}),
    ("Vienna", "capital",  { "geography": "continent", "country": "Austria", "eu": "member"}),
    ("Belgrade", "capital",  { "geography": "continent", "country": "Serbia", "eu": "member"}),
    ("Ljubljana", "capital",  { "geography": "continent", "country": "Slovenia", "eu": "member"}),

    ("Sarajevo", "capital",  { "geography": "continent", "country": "Bosnia and Herzegovina", "eu": "negociating candidate"}),
    ("Zagreb", "capital",  { "geography": "continent", "country": "Croatia", "eu": "negociating candidate"}),

    ("Tirana", "capital",  { "geography": "continent", "country": "Albania", "eu": "negociating candidate"}),
    ("Podgorica ", "capital",  { "geography": "continent", "country": "Montenegro", "eu": "candidate"}),
     ("Chisinau", "capital",  { "geography": "continent", "country": "Moldova", "eu": "candidate"}),
   ("Skopje", "capital",  { "geography": "continent", "country": "North Macedonia", "eu": "candidate"}),
    ("Kyiv", "capital",  { "geography": "continent", "country": "Ukrain", "eu": "candidate"}),
    ("Pristina", "capital",  { "geography": "continent", "country": "Kosovo", "eu": "applicants"}),
    ("Tiblisi", "capital",  { "geography": "continent", "country": "Georgia", "eu": "applicants"}),
    ("Ankara", "capital",  { "geography": "continent", "country": "Turkey", "eu": "stalled candidate"}),
    ("Istanbul", "city",  { "geography": "continent", "country": "Turkey", "eu": "stalled candidate"})
]


eu_connections = [
    ("Helsinki", "carFerry", "Tallinn", {
     'time': 2.0, 'costs': 100, 'km': 86, 'trees': 95}),
    ("Helsinki", "walkFerry", "Tallinn", {
     'time': 2.15, 'costs': 30, 'km': 1, 'trees': 0}),
    ("Helsinki", "fly", "Tallinn", {
     'time': 2.15, 'costs': 203, 'km': 82, 'trees': 299}),
    ("Helsinki", "carFerry", "Stockholm", {
     'time': 17.45, 'costs': 210, 'km': 479, 'trees': 530}),
    ("Helsinki", "busWalkFerry", "Stockholm", {
     'time': 14.29, 'costs': 206, 'km': 479, 'trees': 541}),
    ("Helsinki", "fly", "Stockholm", {
     'time': 3.57, 'costs': 157, 'km': 396, 'trees': 1666}),
    ("Tallinn", "train", "Riga", {'time': 6.52,
     'costs': 27, 'km': 350, 'trees': 154}),
    ("Tallinn", "car", "Riga", {'time': 4.15,
     'costs': 83, 'km': 308, 'trees': 341}),
    ("Tallinn", "bus", "Riga", {'time': 4.25,
     'costs': 40, 'km': 308, 'trees': 348}),
    ("Tallinn", "fly", "Riga", {'time': 2.36,
     'costs': 152, 'km': 280, 'trees': 1071}),
    ("Riga", "car", "Vilnius", {'time': 3.34,
     'costs': 75, 'km': 293, 'trees': 324}),
    ("Riga", "bus", "Vilnius", {'time': 4.24,
     'costs': 22, 'km': 293, 'trees': 331}),
    ("Riga", "fly", "Vilnius", {'time': 2.20,
     'costs': 184, 'km': 262, 'trees': 1019}),
    ("Vilnius", "train", "Warsaw", {
     'time': 9.03, 'costs': 75, 'km': 624, 'trees': 262}),
    ("Vilnius", "car", "Warsaw", {'time': 5.39,
     'costs': 111, 'km': 524, 'trees': 580}),
    ("Vilnius", "bus", "Warsaw", {'time': 8.12,
     'costs': 65, 'km': 574, 'trees': 592}),
    ("Vilnius", "fly", "Warsaw", {'time': 4.11,
     'costs': 229, 'km': 399, 'trees': 1823}),
    ("Warsaw", "train", "Prague", {
     'time': 9.03, 'costs': 75, 'km': 624, 'trees': 262}),
    ("Warsaw", "car", "Prague", {'time': 6.39,
     'costs': 173, 'km': 524, 'trees': 580}),
    ("Warsaw", "bus", "Prague", {'time': 10.30,
     'costs': 45, 'km': 574, 'trees': 592}),
    ("Warsaw", "fly", "Prague", {'time': 4.11,
     'costs': 197, 'km': 399, 'trees': 1823}),
    ("Warsaw", "car", "Bratislava", {
     'time': 6.42, 'costs': 167, 'km': 657, 'trees': 742}),
    ("Warsaw", "bus", "Bratislava", {
     'time': 12.10, 'costs': 85, 'km': 657, 'trees': 727}),

    ("Vienna", "train", "Bratislava", {
     "time": 1.06, "costs": 23, "km": 69, "trees": 39}),
    ("Vienna", "bus", "Bratislava", {
     "time": 1.16, "costs": 14, "km": 79, "trees": 89}),
    ("Vienna", "ferry", "Bratislava", {
     "time": 1.15, "costs": 44, "km": 60, "trees": 44}),
    ("Vienna", "car", "Bratislava", {
     "time": 0.53, "costs": 21, "km": 79, "trees": 87}),

    ("Copenhagen", "fly", "Stockholm", {
     "time": 3.57, "costs": 167, "km": 672, "trees": 3499}),

    ("Copenhagen", "train", "Warsaw", {
     "time": 5.15, "costs": 75, "km": 69, "trees": 39}),
    ("Copenhagen", "bus", "Warsaw", {
     "time": 9.25, "costs": 80, "km": 79, "trees": 89}),
    ("Copenhagen", "fly", "Warsaw", {
     "time": 4.07, "costs": 321, "km": 60, "trees": 44}),
    ("Copenhagen", "car", "Warsaw", {
     "time": 6.46, "costs": 195, "km": 79, "trees": 87}),

    ("Vienna", "train", "Prague", {
     "time": 4.09, "costs": 24, "km": 69, "trees": 39}),
    ("Vienna", "bus", "Prague", {
     "time": 4.15, "costs": 45, "km": 79, "trees": 89}),
    ("Vienna", "fly", "Prague", {"time": 2.38,
     "costs": 150, "km": 60, "trees": 44}),
    ("Vienna", "car", "Prague", {
     "time": 3.31, "costs": 91, "km": 79, "trees": 87}),

    ("Vienna", "train", "Budapest", {
     "time": 2.25, "costs": 65, "km": 220, "trees": 122}),
    ("Vienna", "bus", "Budapest", {
     "time": 3.20, "costs": 32, "km": 214, "trees": 276}),
    ("Vienna", "fly", "Budapest", {"time": 2.38,
     "costs": 223, "km": 214, "trees": 849}),
    ("Vienna", "car", "Budapest", {
     "time": 2.21, "costs": 66, "km": 244, "trees": 270}),

    ("Vienna", "train", "Ljubljana", {
     "time": 5.59, "costs": 85, "km": 383, "trees": 191}),
    ("Vienna", "bus", "Ljubljana", {
     "time": 5.09, "costs": 50, "km": 383, "trees": 433}),
    ("Vienna", "car", "Ljubljana", {
     "time": 2.21, "costs": 103, "km": 383, "trees": 424}),

    ("Budapest", "train", "Ljubljana", {
     "time": 7.35, "costs": 21, "km": 470, "trees": 230}),
    ("Budapest", "bus", "Ljubljana", {
     "time": 6.05, "costs": 45, "km": 460, "trees": 520}),
    ("Budapest", "car", "Ljubljana", {
     "time": 4.21, "costs": 8, "km": 460, "trees": 509}),

    ("Budapest", "train", "Zagreb", {
     "time": 6.35, "costs": 32, "km": 341, "trees": 170}),
    ("Budapest", "bus", "Zagreb", {
     "time": 4.30, "costs": 55, "km": 341, "trees": 485}),
    ("Budapest", "car", "Zagreb", {"time": 3.19,
     "costs": 117, "km": 341, "trees": 377}),

    ("Budapest", "train", "Bucarest", {
     "time": 15.39, "costs": 60, "km": 904, "trees": 417}),
    ("Budapest", "bus", "Bucarest", {
        "time": 17.08, "costs": 85, "km": 934, "trees": 942}),
    ("Budapest", "car", "Bucarest", {
        "time": 9.11, "costs": 209, "km": 834, "trees": 923}),
    ("Budapest", "fly", "Bucarest", {
        "time": 4.38, "costs": 262, "km": 643, "trees": 2901}),

    ("Sofia", "train", "Bucarest", {
        "time": 10.10, "costs": 26, "km": 385, "trees": 191}),
    ("Sofia", "bus", "Bucarest", {
        "time": 8.30, "costs": 30, "km": 363, "trees": 433}),
    ("Sofia", "car", "Bucarest", {
        "time": 4.05, "costs": 89, "km": 383, "trees": 424}),
    ("Sofia", "fly", "Bucarest", {
        "time": 2.26, "costs": 359, "km": 296, "trees": 1332}),

    ("Sofia", "train", "Athens", {
        "time": 13.49, "costs": 73, "km": 799, "trees": 399}),
    ("Sofia", "bus", "Athens", {"time": 11.30,
                                "costs": 75, "km": 798, "trees": 902}),
    ("Sofia", "car", "Athens", {"time": 8.03,
                                "costs": 240, "km": 798, "trees": 883}),
    ("Sofia", "fly", "Athens", {"time": 4.22,
                                "costs": 186, "km": 527, "trees": 2776}),

    ("Athens", "fly", "Nicosia", {
        "time": 5.48, "costs": 116, "km": 915, "trees": 7051}),

    ("Athens", "fly", "Valletta", {
        "time": 4.54, "costs": 163, "km": 852, "trees": 5312}),

    ("Vienna", "train", "Zagreb", {
        "time": 6.27, "costs": 85, "km": 268, "trees": 188}),
    ("Vienna", "bus", "Zagreb", {"time": 4.50,
                                 "costs": 45, "km": 268, "trees": 425}),
    ("Vienna", "car", "Zagreb", {"time": 4.45,
                                 "costs": 101, "km": 268, "trees": 416}),
    ("Vienna", "fly", "Zagreb", {"time": 2.47,
                                 "costs": 232, "km": 818, "trees": 1308}),


    ("Amsterdam", "train", "Bruxelles", {
        'time': 8.19, 'costs': 197, 'km': 770, 'trees': 368}),
    ("Amsterdam", "bus", "Bruxelles", {
        "time": 2.35, "costs": 27, "km": 750, "trees": 832}),
    ("Amsterdam", "car", "Bruxelles", {
        "time": 2.21, "costs": 61, "km": 736, "trees": 814}),
    ("Amsterdam", "fly", "Bruxelles", {
        "time": 2.13, "costs": 217, "km": 512, "trees": 2560}),

    ("Bruxelles", "train", "Paris", {
        "time": 1.52, "costs": 175, "km": 323, "trees": 161}),
    ("Bruxelles", "bus", "Paris", {
        "time": 4.37, "costs": 45, "km": 323, "trees": 365}),
    ("Bruxelles", "car", "Paris", {
        "time": 2.56, "costs": 79, "miles": 188, "km": 323, "trees": 357}),
    ("Bruxelles", "fly", "Paris", {
        "time": 4.01, "costs": 273, "km": 276, "trees": 1124}),

    ("Bruxelles", "train", "Berlin", {
        "time": 8.35, "costs": 170, "km": 767, "trees": 383}),
    ("Bruxelles", "bus", "Berlin", {
        "time": 12, "costs": 45, "km": 767, "trees": 867}),
    ("Bruxelles", "car", "Berlin", {
        "time": 6.50, "costs": 199, "miles": 478, "km": 767, "trees": 849}),
    ("Bruxelles", "fly", "Berlin", {
        "time": 4.31, "costs": 220, "km": 651, "trees": 2668}),


    ("Bruxelles", "train", "Luxembourg", {
        "time": 2.53, "costs": 35, "km": 229, "trees": 114}),
    ("Bruxelles", "bus", "Luxembourg", {
     "time": 2.40, "costs": 29, "km": 229, "trees": 259}),
    ("Bruxelles", "car", "Luxembourg", {
     "time": 2.11, "costs": 65, "km": 229, "trees": 253}),

    ("Amsterdam", "train", "Berlin", {
     "time": 6.20, "costs": 179, "km": 656, "trees": 328}),
    ("Amsterdam", "bus", "Berlin", {
        "time": 9.40, "costs": 80, "km": 656, "trees": 741}),
    ("Amsterdam", "car", "Berlin", {
        "time": 6.10, "costs": 196, "miles": 409, "km": 656, "trees": 726}),
    ("Amsterdam", "fly", "Berlin", {
        "time": 4.15, "costs": 217, "km": 577, "trees": 2282}),

    ("Zagreb", "bus", "Sarajevo", {
        "time": 6.15, "costs": 55, "km": 375, "trees": 424}),
    ("Zagreb", "car", "Sarajevo", {
        "time": 4.41, "costs": 94, "km": 375, "trees": 415}),
    ("Zagreb", "fly", "Sarajevo", {
        "time": 2.22, "costs": 209, "km": 283, "trees": 1304}),

    ("Belgrade", "bus", "Sarajevo", {
        "time": 7.17, "costs": 26, "km": 347, "trees": 392}),
    ("Belgrade", "car", "Sarajevo", {
        "time": 4.34, "costs": 72, "km": 347, "trees": 384}),
    ("Belgrade", "fly", "Sarajevo", {
        "time": 3.39, "costs": 125, "km": 206, "trees": 1207}),

    ("Rome", "train", "Vienna", {"time": 2.25,
                                 "costs": 65, "km": 220, "trees": 122}),
    ("Rome", "bus", "Vienna", {"time": 3.20,
     "costs": 32, "km": 214, "trees": 276}),
    ("Rome", "fly", "Vienna", {"time": 2.38,
     "costs": 223, "km": 214, "trees": 849}),
    ("Rome", "car", "Vienna", {"time": 2.21,
     "costs": 66, "km": 244, "trees": 270}),

    ("Rome", "fly", "Valletta", {"time": 2.38,
     "costs": 223, "km": 214, "trees": 849}),

    ("Berlin", "train", "Warsaw", {
        "time": 7.23, "costs": 40, "km": 592, "trees": 286}),
    ("Berlin", "bus", "Warsaw", {"time": 7.35,
     "costs": 70, "km": 572, "trees": 646}),
    ("Berlin", "fly", "Warsaw", {"time": 4.23,
     "costs": 232, "km": 518, "trees": 1900}),
    ("Berlin", "car", "Warsaw", {"time": 5.07,
     "costs": 137, "km": 572, "trees": 633}),

    ("Berlin", "train", "Copenhagen", {
     "time": 10.01, "costs": 46, "km": 622, "trees": 311}),
    ("Berlin", "bus", "Copenhagen", {
     "time": 8.10, "costs": 70, "km": 622, "trees": 703}),
    ("Berlin", "fly", "Copenhagen", {
     "time": 4.01, "costs": 179, "km": 355, "trees": 2164}),
    ("Berlin", "car", "Copenhagen", {
     "time": 11.05, "costs": 128, "km": 622, "trees": 688}),

    ("Berlin", "train", "Luxembourg", {
     "time": 7.23, "costs": 40, "km": 1013, "trees": 443}),
    ("Berlin", "bus", "Luxembourg", {
     "time": 11.05, "costs": 95, "km": 887, "trees": 1002}),
    ("Berlin", "fly", "Luxembourg", {
     "time": 4.23, "costs": 444, "km": 590, "trees": 3085}),
    ("Berlin", "car", "Luxembourg", {
     "time": 6.40, "costs": 181, "km": 887, "trees": 981}),

    ("Berlin", "train", "Vienna", {"time": 8.40,
     "costs": 205, "km": 5681, "trees": 340}),
    ("Berlin", "bus", "Vienna", {"time": 8.59,
     "costs": 80, "km": 681, "trees": 769}),
    ("Berlin", "fly", "Vienna", {"time": 4.12,
     "costs": 217, "km": 524, "trees": 2369}),
    ("Berlin", "car", "Vienna", {"time": 6.45,
     "costs": 184, "km": 681, "trees": 754}),

    ("Berlin", "train", "Prague", {"time": 5.09,
     "costs": 148, "km": 364, "trees": 174}),
    ("Berlin", "bus", "Prague", {"time": 4.25,
     "costs": 50, "km": 349, "trees": 394}),
    ("Berlin", "fly", "Prague", {"time": 6.06,
     "costs": 314, "km": 282, "trees": 1214}),
    ("Berlin", "car", "Prague", {"time": 3.21,
     "costs": 89, "km": 349, "trees": 386}),


    ("Paris", "train", "Luxembourg", {
     "time": 2.21, "costs": 90, "km": 428, "trees": 204}),
    ("Paris", "bus", "Luxembourg", {
     "time": 5.10, "costs": 50, "km": 408, "trees": 461}),
    ("Paris", "fly", "Luxembourg", {
     "time": 3.59, "costs": 167, "km": 590, "trees": 1419}),
    ("Paris", "car", "Luxembourg", {
     "time": 3.26, "costs": 97, "km": 408, "trees": 451}),

    ("Paris", "train", "Madrid", {"time": 9.24,
     "costs": 360, "km": 2123, "trees": 844}),
    ("Paris", "bus", "Madrid", {"time": 16.45,
     "costs": 290, "km": 1689, "trees": 1908}),
    ("Paris", "fly", "Madrid", {"time": 5.04,
     "costs": 197, "km": 1298, "trees": 5875}),
    ("Paris", "car", "Madrid", {"time": 11.52,
     "costs": 334, "km": 1689, "trees": 1689}),

    ("Paris", "train", "Rome", {"time": 9.33,
     "costs": 263, "km": 1474, "trees": 710}),
    ("Paris", "bus", "Rome", {"time": 16.45,
     "costs": 290, "km": 1454, "trees": 1606}),
    ("Paris", "fly", "Rome", {"time": 5.18,
     "costs": 268, "km": 1106, "trees": 4943}),
    ("Paris", "car", "Rome", {"time": 13.26,
     "costs": 374, "km": 1424, "trees": 1572}),

    ("Paris", "fly", "Dublin", {"time": 4.57,
     "costs": 174, "km": 782, "trees": 3712}),
    ("Paris", "carFerry", "Dublin", {
     "time": 22.53, "costs": 178, "km": 1067, "trees": 1181}),

    ("Paris", "fly", "Valletta", {"time": 5.35,
     "costs": 319, "km": 1740, "trees": 8550}),

    ("Nicosia", "fly", "Valletta", {
        "time": 5.57, "costs": 142, "km": 1719, "trees": 10140}),

    ("Rome", "fly", "Athens", {"time": 5.30,
                               "costs": 199, "km": 1053, "trees": 4425}),
    ("Rome", "carFerry", "Athens", {
        "time": 18.28, "costs": 397, "km": 1272, "trees": 1407}),

    ("Bruxelles", "fly", "Dublin", {
        "time": 4.24, "costs": 141, "km": 776, "trees": 3339}),

    ("Amsterdam", "fly", "Dublin", {
        "time": 4.25, "costs": 238, "km": 757, "trees": 3938}),

    ("Madrid", "train", "Lisbon", {
     "time": 10.15, "costs": 71, "km": 696, "trees": 313}),
    ("Madrid", "bus", "Lisbon", {"time": 7.31,
     "costs": 75, "km": 626, "trees": 707}),
    ("Madrid", "fly", "Lisbon", {"time": 4.07,
     "costs": 75, "km": 504, "trees": 2178}),
    ("Madrid", "car", "Lisbon", {"time": 5.50,
     "costs": 165, "km": 626, "trees": 693}),

    ("Madrid", "busFerry", "Dublin", {
     "time": 39.21, "costs": 570, "km": 2314, "trees": 2615}),
    ("Madrid", "fly", "Dublin", {"time": 5.38,
     "costs": 489, "km": 1452, "trees": 8050}),
    ("Madrid", "carFerry", "Dublin", {
     "time": 34.48, "costs": 614, "km": 2314, "trees": 2560}),

    ("Madrid", "fly", "Valletta", {"time": 5.38,
     "costs": 110, "km": 1655, "trees": 10391}),

    ("Lisbon", "fly", "Valletta", {
        "time": 5.49, "costs": 149, "km": 2098, "trees": 12318}),

    ("Rome", "trainBusFerry", "Valletta", {
        "time": 16.36, "costs": 159, "km": 1063, "trees": 531}),
    ("Rome", "BusFerry", "Valletta", {
        "time": 18.50, "costs": 188, "km": 1063, "trees": 1201}),
    ("Rome", "fly", "Valletta", {"time": 4.29,
                                "costs": 190, "km": 682, "trees": 3698}),

    ("Rome", "bus", "Ljubljana", {
        "time": 10.39, "costs": 90, "km": 752, "trees": 850}),
    ("Rome", "car", "Ljubljana", {
        "time": 7.01, "costs": 198, "km": 752, "trees": 832}),
    ("Rome", "fly", "Zagreb", {"time": 4.34,
                               "costs": 174, "km": 517, "trees": 3079}),



    ("Sofia", "bus", "Skopje", {
        "time": 5, "costs": 29, "km": 244, "trees": 276}),
    ("Sofia", "car", "Skopje", {
        "time": 3.03, "costs": 55, "km": 244, "trees": 270}),

    ("Sofia", "train", "Ankara", {
        "time": 17.16, "costs": 42, "km": 1225, "trees": 500}),
    ("Sofia", "car", "Ankara", {
        "time": 9.42, "costs": 228, "km": 1000, "trees": 1106}),
    ("Sofia", "bus", "Ankara", {
        "time": 18.31, "costs": 47, "km": 1000, "trees": 1130}),
    ("Sofia", "fly", "Ankara", {
        "time": 7.09, "costs": 217, "km": 854, "trees": 3479}),

    ("Sofia", "bus", "Istanbul", {
        "time": 9.30, "costs": 28, "km": 599, "trees": 654}),
    ("Sofia", "car", "Istanbul", {
        "time": 5.10, "costs": 124, "km": 579, "trees": 641}),
    ("Sofia", "train", "Istanbul", {
        "time": 11.33, "costs": 27, "km": 579, "trees": 289}),
    ("Sofia", "fly", "Istanbul", {
        "time": 4.03, "costs": 374, "km": 505, "trees": 2014}),

    ("Athene", "car", "Ankara", {
        "time": 16.05, "costs": 467, "km": 1546, "trees": 1711}),
    ("Athene", "bus", "Ankara", {
        "time": 21.09, "costs": 141, "km": 1546, "trees": 1747}),
    ("Athene", "fly", "Ankara", {
        "time": 7.17, "costs": 164, "km": 818, "trees": 5378}),

    ("Athene", "bus", "Istanbul", {
        "time": 14, "costs": 120, "km": 599, "trees": 1241}),
    ("Athene", "car", "Istanbul", {
        "time": 11.33, "costs": 328, "km": 579, "trees": 1215}),
    ("Athene", "fly", "Istanbul", {
        "time": 4.36, "costs": 191, "km": 505, "trees": 3820}),

    ("Athene", "bus", "Tirana", {
        "time": 15.45, "costs": 115, "km": 749, "trees": 821}),
    ("Athene", "car", "Tirana", {
        "time": 8.45, "costs": 219, "km": 727, "trees": 804}),
    ("Athene", "fly", "Tirana", {
        "time": 4.36, "costs": 174, "km": 501, "trees": 2529}),

    ("Athene", "bus", "Skopje", {
        "time": 12.45, "costs": 89, "km": 696, "trees": 786}),
    ("Athene", "car", "Skopje", {
        "time": 7, "costs": 210, "km": 696, "trees": 770}),
    ("Athene", "fly", "Skopje", {
        "time": 5.52, "costs": 285, "km": 489, "trees": 2421}),

    ("Sofia", "bus", "Ankara", {
        "time": 5, "costs": 29, "km": 244, "trees": 276}),
    ("Sofia", "car", "Ankara", {
        "time": 3.03, "costs": 55, "km": 244, "trees": 270}),

    ("Sofia", "trainFerryTrain", "Tiblisi", {
        "time": 68.54, "costs": 394, "km": 2227, "trees": 1094}),
    ("Sofia", "busFerryMinibus", "Tiblisi", {
        "time": 75.16, "costs": 264, "km": 2189, "trees": 2473}),

    ("Pristina", "bus", "Tirana", {
        "time": 4, "costs": 17, "km": 254, "trees": 287}),
    ("Pristina", "car", "Tirana", {
        "time": 3.14, "costs": 66, "km": 254, "trees": 281}),

    ("Pristina", "bus", "Belgrade", {
        "time": 6.30, "costs": 22, "km": 538, "trees": 608}),
    ("Pristina", "car", "Belgrade", {
        "time": 3.59, "costs": 79, "km": 538, "trees": 595}),

    ("Sofia", "bus", "Belgrade", {
        "time": 6.30, "costs": 30, "km": 434, "trees": 490}),
    ("Skopje", "car", "Belgrade", {
        "time": 3.59, "costs": 89, "km": 434, "trees": 480}),
    ("Skopje", "fly", "Belgrade", {
        "time": 4.03, "costs": 259, "km": 321, "trees": 1510}),

    ("Pristina", "bus", "Podgorica", {
        "time": 7.55, "costs": 17, "km": 257, "trees": 290}),
    ("Pristina", "car", "Podgorica", {
        "time":4.13, "costs": 55, "km": 257, "trees": 284}),

    ("Pristina", "train", "Skopje", {
        "time": 2.42, "costs": 25, "km": 112, "trees": 46}),
    ("Pristina", "car", "Skopje", {
        "time": 1.12, "costs": 21, "km": 92, "trees": 101}),

    ("Sarajevo", "bus", "Podgorica", {
        "time": 7.30, "costs": 30, "km": 229, "trees": 259}),
    ("Sarajevo", "car", "Podgorica", {
        "time": 3.52, "costs": 53, "km": 229, "trees": 253}),

    ("Bucarest", "train", "Chisinau", {
        "time": 12.50, "costs": 60, "km": 627, "trees": 218}),
    ("Bucarest", "bus", "Chisinau", {
        "time": 8.30, "costs": 35, "km": 437, "trees": 494}),
    ("Bucarest", "car", "Chisinau", {
        "time": 6.23, "costs": 100, "km": 437, "trees": 483}),
    ("Bucarest", "fly", "Chisinau", {
        "time": 6.44, "costs": 221, "km": 358, "trees": 1520}),

    ("Ankara", "bus", "Tiblisi", {
        "time": 19.55, "costs": 70, "km": 1327, "trees": 1556}),
    ("Ankara", "car", "Tiblisi", {
        "time": 18.14, "costs": 229, "km": 1327, "trees": 1524}),
    ("Ankara", "fly", "Tiblisi", {
        "time": 5.16, "costs": 184, "km": 1024, "trees": 4790}),

    ("Kyiv", "train", "Chisinau", {
        "time": 17.54, "costs": 55, "km": 627, "trees": 218}),
    ("Kyiv", "bus", "Chisinau", {
        "time": 10.54, "costs": 45, "km": 437, "trees": 494}),
    ("Kyiv", "car", "Chisinau", {
        "time": 5.40, "costs": 95, "km": 437, "trees": 483}),

    ("Bucarest", "bus", "Kyiv", {
        "time": 19.42, "costs": 53, "km": 916, "trees": 1024}),
    ("Bucarest", "car", "Kyiv", {
        "time": 11.52, "costs": 221, "km": 916, "trees": 1014}),

    ("Warsaw", "train", "Kyiv", {
        "time": 19.20, "costs": 68, "km": 855, "trees": 407}),
    ("Warsaw", "bus", "Kyiv", {
        "time": 16.35, "costs": 95, "km": 835, "trees": 921}),
    ("Warsaw", "car", "Kyiv", {
        "time": 10.12, "costs": 163, "km": 815, "trees": 902}),

    ("Kyiv", "trainFerryTrain", "Tiblisi", {
        "time": 57.16, "costs": 484, "km": 1846, "trees": 922}),
    ("Kyiv", "busFerryMinibus", "Tiblisi", {
        "time": 58.33, "costs": 411, "km": 1846, "trees": 2086}),

    ("Kyiv", "car", "Bratislava", {
        "time": 14.16, "costs": 331, "km": 437, "trees": 483}),

    ("Kyiv", "train", "Budapest", {
        "time": 20.04, "costs": 300, "km": 151, "trees": 158}),
    ("Kyiv", "bus", "Budapest", {
        "time": 21.55, "costs": 126, "km": 1117, "trees": 1262}),
    ("Kyiv", "car", "Budapest", {
        "time": 14.15, "costs": 282, "km": 1117, "trees": 1236}),
]


route_types = ["", "w", "time", "costs", "km", "trees",
"{SQRT( pow(costs,2)+pow(time,2) ) }",
"{SQRT( pow(costs,2)+pow(trees,2) ) }",
"{SQRT( pow(trees,2)+pow(time,2) ) }",
"{SQRT( pow(costs,2)+pow(km,2)+pow(time,2) )}",
"{SQRT( pow(trees,2)+pow(km,2)+pow(time,2) )}",
"{SQRT( pow(costs,2)+pow(km,2)+pow(time,2)+pow(trees,2) )}",
]  # ["trees"] #


def gremlin_match_setup(cluster_id, controller, data, index):
    for capital in eu_capitals:
        addV = "g:addV({},capital)".format(capital[0])
        if len(capital) >= 3:
            if type(capital[2]) == dict:
                for a in capital[2]:
                    addV += ".property({},'{}')".format(a, capital[2][a])
        print(addV)
        data.execute_command("RXQUERY", addV)
        # city = data.hgetall(capital[0])
        # print("{}: {}".format(capital[0],city))
    for connection in eu_connections:
        weighted = 0.0
        properties = ""
        if len(connection) >= 4:
            if type(connection[3]) == dict:
                for a in connection[3]:
                    properties += ".property({},{})".format(a, connection[3][a])
                    if a == "time":
                        h = int(connection[3][a])
                        m = connection[3][a] - h
                        weighted += math.pow(h*60+m, 2)
                    else:
                        weighted += math.pow(connection[3][a], 2)

        addE = "g:predicate({}).subject({}).object({}).property(w,{}).{}".format(
            connection[1], connection[0], connection[2], math.sqrt(weighted), properties)
        print(addE)
        print(data.execute_command("RXQUERY", addE))

def gremlin_match(cluster_id, controller, data, index):
    for a in eu_capitals[0:]:
        for b in eu_capitals[0:]:
            if a[0] == b[0]:
                continue
            for t in route_types:
                if len(t) == 0:
                    print("\nGetting route from {} to {}\n".format(a[0], b[0]))
                    print(data.execute_command(
                        "RXQUERY", "g:match({},{})".format(a[0], b[0])))
                else:
                    print("\nGetting route from {} to {} optimized by {}\n".format(a, b, t))
                    print(data.execute_command(
                        "RXQUERY", "g:minimize({}).match({},{})".format(t, a[0], b[0])))
                    print(data.execute_command(
                        "RXQUERY", "g:maximize({}).match({},{})".format(t, a[0], b[0])))
    # exit()

def gremlin__multi_match(cluster_id, controller, data, index):
    for a in eu_capitals[0:]:
        for b in eu_capitals[0:]:
            for c in eu_capitals[0:]:
                if a[0] == b[0] or b[0] == c[0] or c[0] == a[0]:
                    continue
                for t in route_types:
                    if len(t) == 0:
                        print("\nGetting route from {} to {} and {}\n".format(a[0], b[0], c[0]))
                        print(data.execute_command(
                            "RXQUERY", "g:match({},{})".format(a[0], b[0], c[0])))
                    else:
                        print("\nGetting route from {} to {} and {} optimized by {}\n".format(a, b, c, t))
                        print(data.execute_command(
                            "RXQUERY", "g:minimize({}).match({},{})".format(t, a[0], b[0], c[0])))
    # exit()
