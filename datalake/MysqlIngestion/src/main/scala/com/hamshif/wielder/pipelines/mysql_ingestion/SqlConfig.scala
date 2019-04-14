package com.hamshif.wielder.pipelines.mysql_ingestion

case class SqlConfig(
                      host: String = "",
                      port: Int = 3306,
                      db: String = "",
                      tables: String = "user",
                      user: String = "admin",
                      password: String = "admin"
                     )
