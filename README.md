WildLines is A home project for Spark ETL's run on Hadoop & locally
The project also houses a common module which is reusable in other scala java and JVM contexts

Pipelines is a multi Pom project enabling elastic mix and match lean dependency Jar builds withing one version
The multi-Pom architecture is intended to divorce Maven from version control leaving versioning to Git
facilitating versioning in a multi language heterogeneous environment e.g.
interpreted Python code for airflow DAG's which has nothing to do with Maven

The pipelines are a work in progress and as of yet lack testing

Build
==
common: 
```
-Dmaven.test.skip=true clean install -f pom.xml -f pom.xml
```
modules: 
```
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
```

TODO reduce build time by allowing scope and multiple jars

Uses a commandline arg parser DatalakeArgParser to call the job with arguments e.g.
You can add this to the arguments in the run configurations


local cmd args
```
--fs /Users/gbar/ --bucket ram/short --master local
```

```
gcloud dataproc jobs submit spark   --project rtp-gcp-poc --cluster g-qe-gid-dataproc-0002-a   --region global   --jars gs://gid-ram/fastq-1.0.0-SNAPSHOT.jar   --class FastQ   -- --fs gs:// --bucket gid-ram/ram -i fastq -o fastq 
```