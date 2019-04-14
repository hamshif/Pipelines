package com.hamshif.wielder.wild

import com.hamshif.wielder.wild.DevEnv.DevEnv
import com.hamshif.wielder.wild.RuntimeEnv.RuntimeEnv


/**
  * @author Gideon Bar
  *
  */
case class DatalakeConfig(

                        confFile: String = "/nowhere",
                        env: DevEnv = DevEnv.QE,
                        runtimeEnv: RuntimeEnv = RuntimeEnv.Dataproc,
                        fsPrefix: String = "gs://",
                        bucketName: String = "datalake-dev-poc1",
                        ingestionType: String = "parquet",
                        syncType: String = "parquet",
                        sparkMaster: String = "yarn",
                        subDirs: String = "full,incremental",
                        kafkaTopics: String = "change_to_None",
                        mySqlTable: String = "change_to_None"
)
