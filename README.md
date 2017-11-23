# spark-lundstrom
Makva &amp; Lundstrom model execution using Apache Spark or BSC COMPSs logs

Log folder directory should be configured at config.json file. See an example at config.json.sample.

## Usage
python run.py -n {NODES} -c {CORES PER NODE} -r {RAM GB} -d {DATASET SIZE} -q {ALGORITHM OR APPLICATION} -p {PLATFORM}

| Param  | Description |
| ------------- | ------------- |
| -p | compss or spark |
| -k | param should be used when user wants do predict different number of cores. The base logs are defined in config.json file, while the config to predict is as the params. |
| -s | Add stages' details on output. 
| -t | (Only for COMPSs) This param generates stage's intermediary files with task times. The stage files are generated under ./temp folder. |

e.g.: python run.py -n 8 -c 4 -r 8G -d 2M -q kmeans -p compss

## Generating COMPSs intermediary files
Include the -t parameter when calling the run.py file for -p compss. The intermediary files will be generated at ./temp folder, separeted by stages containing the task times separeted by new lines.

e.g.: python run.py -n 2 -c 96 -r 8G -d 2M -q kmeans -p compss -t