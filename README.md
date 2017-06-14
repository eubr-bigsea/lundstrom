# spark-lundstrom
Makva &amp; Lundstrom model execution using Apache Spark or BSC COMPSs logs

Log folder directory should be configured at config.json file. See an example at config.json.sample.

## Usage
python run.py {NODES} {CORES PER NODE} {RAM GB} {DATASET SIZE} {ALGORITHM OR APPLICATION} {PLATFORM}

e.g.: python run.py 8 4 8G 2M kmeans compss
