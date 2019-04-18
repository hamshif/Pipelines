package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.DatalakeArgParser

trait FastqArgParser extends DatalakeArgParser {

  def getSpecificConf(args: Array[String]): FastqConfig = {

//    args.map(arg => println(s"arg: $arg"))

    sParser.parse(args, FastqConfig()) match {

      case Some(config) => config
      case None => throw new Exception("bad config")
    }
  }

  val sParser = new scopt.OptionParser[FastqConfig](this.getClass.getName) {

    override def errorOnUnknownArgument = false

    head("scopt", "3.x")

    opt[Boolean]("verbose")
      .action((x, c) => c.copy(debugVerbose = x))
      .text("Liberally use dataframe show and count for developing on small data sets. " +
        "Defaults to false to minimize cache and runtime")

    opt[Int]("bases")
      .action((x, c) => c.copy(minBases = x))
      .text("Minimum bases for read 2 (R2) to be considered similar. " +
        "defaults to 5 bases")

    opt[Int]("fidelity")
      .action((x, c) => c.copy(minFidelityScore = x))
      .text("Minimum transcription fidelity score for read 2 (R2) not to be filtered")

  }
}
