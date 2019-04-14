package com.hamshif.wielder.wild

import java.io.File

import org.joda.time.DateTime

//TODO generalize and test functions to fit wide use cases
// e.g. walk with directory depth limit option
// e.g. walk with options for filtering files or directories
/**
  * @author Gideon Bar
  * A utility for high level File management (does not work for remote filesystems e.g. Hadoop with buckets)
  *
  */
trait FileUtil extends CommonDictionary{

  def getListOfFiles(dir: String): List[File] = {

    val d = new File(dir)

    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def step(d: File, fileFilter: File => Boolean, dirFilter: File => Boolean): (List[File], List[File]) = d match {

    case _ if (d.exists && d.isDirectory) => {

      val v = d.listFiles

      v.foldLeft((List[File](), List[File]())) ((acc, f) => f match {

        case aa if f.isFile && fileFilter(f) =>
          (aa :: acc._1, acc._2)
        case aa if f.isDirectory && dirFilter(f) =>
          println(aa.getPath)
          (acc._1, aa :: acc._2)
        case _ =>
          acc
      })
    }
    case _ =>
      (Nil, Nil)
  }

  /**
    * @
    * returns a tuple
    * ._1  = list of all files in all subdirectories filtered by filter arge
    * ._2  = List of all subdirectories
    */
  def walk(path: String, fileFilter: File => Boolean, dirFilter: File => Boolean): (List[File], List[File]) = {

    val f = new File(path) :: Nil

    val w = walk(List[File](), List[File]() , f, fileFilter, dirFilter)

    (w._1, w._2)
  }


  def walk(files: List[File], dirs: List[File], dir_counter: List[File], fileFilter: File => Boolean, dirFilter: File => Boolean): (List[File], List[File]) = dir_counter match {

    case Nil =>

      (files, dirs)

    case h::t =>

      val s = step(h, fileFilter, dirFilter)

      walk(files ::: s._1, h :: dirs, t ::: s._2, fileFilter, dirFilter)
  }


  val parquetFilter: File => Boolean = gather => gather match {

    case f if f.getName.endsWith(".parquet") =>
      true
    case _ =>
      false
  }

  val dirTimestampFilter: File => Boolean = file => file match {

    case f if f.isDirectory =>

      val name = f.getName

      try {

        val date: DateTime = DateTime.parse(name, dateFormatter)

//        println(s"successfully parsed: $name")
        true
      }
      catch {
        case e =>
//          println(s"couldn't parse $name")
          false
      }

    case _ =>
      false
  }

}


