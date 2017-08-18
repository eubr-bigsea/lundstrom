# spark-lundstrom
Makva &amp; Lundstrom model execution using Apache Spark or BSC COMPSs logs

Log folder directory should be configured at config.json file. See an example at config.json.sample.

## Usage
python run.py -n {NODES} -c {CORES PER NODE} -r {RAM GB} -d {DATASET SIZE} -q {ALGORITHM OR APPLICATION} -p {PLATFORM}

-b param should be used when user wants do predict different number of cores. The base logs are defined in config.json file, while the config to predict is as the params.
#python run.py {NODES} {CORES PER NODE} {RAM GB} {DATASET SIZE} {ALGORITHM OR APPLICATION} {PLATFORM}

e.g.: python run.py 8 4 8G 2M kmeans compss
