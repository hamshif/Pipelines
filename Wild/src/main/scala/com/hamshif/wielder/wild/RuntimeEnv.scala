package com.hamshif.wielder.wild

/**
  * @author Gideon Bar
  *
  */
object RuntimeEnv extends Enumeration {

  type RuntimeEnv = Value

  val Local = Value("local")
  val Dataproc = Value("dataproc")
  val OnPrem = Value("onprem")
}
