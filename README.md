# Earthquake Co-occurrence Analysis — Scala + Spark on Google Cloud DataProc

## Prerequisites

- [sbt](https://www.scala-sbt.org/) ≥ 1.9
- Console GCP
- A Google Cloud project with DataProc

---

## 1. Build the JAR

Make sure the assembly plugin is added to `project/plugins.sbt`:

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
```

Then build:

```bash
sbt assembly
```
The output file is inside the target/ folder (target/scala-2.12/ProgettoScalable.jar)

---

## 2. Upload dataset and JAR to GCS

```bash
# Create a bucket (once) via Google Cloud Console
gcloud storage buckets create gs://earthquake-cc --location=europe-west1
```
Upload dataset via GCP interface:
1. Search for Buckets in the Search Bar
2. Select the earthquake-cc bucket
3. Create a new folder data/
4. Upload the dataset dataset-full.csv

Upload JAR via GCP interface:
1. Search for Buckets in the Search Bar
2. Select the earthquake-cc bucket
3. Create a new folder code/
4. Upload the JAR ProgettoScalable.jar

---

## 3. Create a DataProc Cluster

```bash
  gcloud dataproc clusters create earthquake-cluster     
  --region=europe-west1     
  --num-workers 2     
  --master-machine-type=n2-standard-4    
  --worker-machine-type=n2-standard-4     
  --master-boot-disk-size 240     
  --worker-boot-disk-size 240     
  --max-idle=15m  
  --enable-component-gateway
```

> Change `--num-workers` to 2,3,4 for scalability experiments.

---

## 4. Submit the Spark Job

```bash
gcloud dataproc jobs submit spark     
--cluster=earthquake-cluster     
--region=europe-west1     
--class=EarthquakeCooccurences     
--jars=gs://earthquake-cc/code/ProgettoScalable.jar     
-- gs://earthquake-cc/data/dataset-full.csv <NUM_PARTITIONS>
```

> Change `<NUM_PARTITIONS>` to 16,24,32,48,64 for experiments.

## 5. Delete the Cluster
```bash
gcloud dataproc clusters deleaate earthquake-cluster --region europe-west1
```
> If the cluster is created with the max-idle option, it gets deleted automatically after 15 minutes of idle.

---

## 6. Expected Output Format

```
2024-03-12
2024-04-01
2024-04-03
((37.5, 15.3), (38.1, 13.4))
Total co-occurrences: 3
Elapsed time: 42.3 s
```

---

## 7. Performance Experiments

The experiments are executed with the following configurations:

| Workers | Partitions |
|---------|------------|
| 2       | 16         | 
| 2       | 32         | 
| 3       | 24         | 
| 3       | 48         | 
| 4       | 32         | 
| 4       | 64         |
