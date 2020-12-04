# Big Data Final Project - 2020 - FPL Analytics

# Repository Structure 
```
+
|----Project
|        |--- master.py
|        |--- ui.py
|        |--- clustering.py
|        |--- stream.py
|        |--- Data
|               |--- final_chemistry.txt
|               |--- final_match_data.txt
|               |--- final_player_profile.json
|               |--- players.csv
|               |--- teams.csv
+
```

### *File/Folder Descriptions*

#### master.py

> 1. Accepts streaming data from port 6100 and performs metric calculations and player-profile updations using Pyspark library during training phase<br>

#### ui.py
> 1. Testing of data is done by passing the input json queries along with this file as command line args. Writes output for a query onto output.json file.

#### clustering.py
> 1. Performs clustering on subset of Data using Pyspark MLLib.

#### stream.py
> 1 . Streams input data in json format onto port 6100 . Compatible with version 3.6 of Python.

#### Data
> 1.final_chemistry.txt : the chemistry metric between players ,obtained till the last successful iteration <br>
> 2.final_match_data.txt : match detail records obtained at the end of every match <br>
> 3.final_player_profile.json : Final Player profile obtained post training phase <br>
> 4.players.csv : Input data regarding players involved/participating in the tournament. <br>
> 5.teams.csv : Input data regarding teams participating in tournament. <br>




