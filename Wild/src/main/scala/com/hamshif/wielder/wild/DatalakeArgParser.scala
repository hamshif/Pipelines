package com.hamshif.wielder.wild

/**
  * @author Gideon Bar
  *
  */
trait DatalakeArgParser {

  def getConf(args: Array[String]): DatalakeConfig = {

    args.map(arg => println(s"arg: $arg"))

    parser.parse(args, DatalakeConfig()) match {

      case Some(config) => config
      case None => throw new Exception("bad config")
    }
  }

  val parser = new scopt.OptionParser[DatalakeConfig](this.getClass.getName) {

    override def errorOnUnknownArgument = false

    head("scopt", "3.x")

    opt
      [String]('c', "conf")
      .valueName("<file>")
//      TODO add check File
      .action( (x, c) => c.copy(confFile = x) )
      .text(s"conf is an optional config file path property")

    opt[String]('e', "env")
      .action((x, c) => c.copy(env = {

        x match {
          case "int" => DevEnv.INT
          case "qe" => DevEnv.QE
          case "st" => DevEnv.STAGE
          case "prod" => DevEnv.PROD
          case _ => DevEnv.QE
        }

      }))
      .text("env is an enumeration of environments e.g. int, qe, st, prod")

    opt[String]('r', "renv")
      .action((x, c) => c.copy(runtimeEnv = {

        x match {
          case "dataproc" => RuntimeEnv.Dataproc
          case "local" => RuntimeEnv.Local
          case "marketo" => RuntimeEnv.MarketoOnPrem
          case _ => RuntimeEnv.Dataproc
        }

      }))
      .text("renv is an enumeration of runtime environments e.g. Dataproc, Local, Marketo On Premises")

    opt[String]('f', "fs")
      .action((x, c) => c.copy(fsPrefix = x))
      .text("File System Prefix e.g. gs:// for Google Storage or a local path for dev")

    opt[String]('b', "bucket")
      .action((x, c) => c.copy(bucketName = x))
      .text("Bucket Name")

    opt[String]( "topics")
      .action((x, c) => c.copy(kafkaTopics = x))
      .text("Kafka topics")

    opt[String]('i', "intype")
      .action((x, c) => c.copy(ingestionType = x))
      .text("Source file type")

    opt[String]('o', "outype")
      .action((x, c) => c.copy(syncType = x))
      .text("Target file type")

    opt[String]("master")
      .action((x, c) => c.copy(sparkMaster = {

        x match {
          case "local" =>
            x + "[*,30]"
          case _ =>
            x
        }
      }))
      .text("Spark context master argument " +
        "https://spark.apache.org/docs/2.3.1/submitting-applications.html#master-urls")

    opt[String]( "subs")
      .action((x, c) => c.copy(subDirs = x))
      .text("CSV sub directories in the target e.g full and incremental")

    opt[String]( "table")
      .action((x, c) => c.copy(subDirs = x))
      .text("SQL table")
  }
}