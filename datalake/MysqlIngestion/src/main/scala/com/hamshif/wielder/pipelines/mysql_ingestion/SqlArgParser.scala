package com.hamshif.wielder.pipelines.mysql_ingestion

import com.hamshif.wielder.wild.DatalakeArgParser

trait SqlArgParser extends DatalakeArgParser {

  def getSpecificConf(args: Array[String]): SqlConfig = {

//    args.map(arg => println(s"arg: $arg"))

    sParser.parse(args, SqlConfig()) match {

      case Some(config) => config
      case None => throw new Exception("bad config")
    }
  }

  val sParser = new scopt.OptionParser[SqlConfig](this.getClass.getName) {

    override def errorOnUnknownArgument = false

    head("scopt", "3.x")

    opt[String]( 'h', "host")
      .action((x, c) => c.copy(host = x))
      .text("SQL host")

    opt[Int]( 'p',"port")
      .action((x, c) => c.copy(port = x))
      .text("Sql Port")

    opt[String]("db")
      .action((x, c) => c.copy(db = x))
      .text("SQL DB")

    opt[String]("tables")
      .action((x, c) => c.copy(tables = x))
      .text("Tables")

    opt[String]( 'u', "usr")
      .action((x, c) => c.copy(user = x))
      .text("SQL user")

    opt[String]("pswd")
      .action((x, c) => c.copy(password = x))
      .text("SQL Password")
  }
}
