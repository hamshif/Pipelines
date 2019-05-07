
local cmd args

```
--fs /Users/gbar/ --bucket ram/short --master local --verbose false
```

dataproc
```
gcloud dataproc jobs submit spark   --project rtp-gcp-poc --cluster g-qe-gid-dataproc-0002-a   --region global   --jars gs://gid-ram/fastq-1.0.0-SNAPSHOT.jar   --class com.hamshif.wielder.pipelines.fastq.FastQ   -- --fs gs:// --bucket gid-ram/ram -i fastq -o fastq 

gcloud dataproc jobs submit spark   --project rtp-gcp-poc --cluster g-qe-gid-dataproc-0002-a   --region global   --jars gs://gid-ram/fastq-exp-1.0.0-SNAPSHOT.jar   --class com.hamshif.wielder.pipelines.fastq.FastQExperiment   -- --fs gs:// --bucket gid-ram/ram -i fastq -o fastq 

```