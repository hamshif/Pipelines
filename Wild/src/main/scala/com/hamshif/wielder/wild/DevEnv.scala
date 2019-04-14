package com.hamshif.wielder.wild

/**
  * @author Gideon Bar
  *
  */
object DevEnv extends Enumeration {

  type DevEnv = Value

  val QE = Value("qe")
  val INT = Value("int")
  val STAGE = Value("st")
  val PROD = Value("prod")
}
