package com.hamshif.wielder.wild

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.joda.time.DateTime

//TODO generalize functions to fit wide use cases
// e.g. walk with directory depth limit option
// e.g. walk with options for filtering files or directories
/**
  * @author Gideon Bar
  * A utility for high level Hadoop File System File management
  *
  */
trait FsUtil extends Logging with CommonDictionary{

  def walkFiles(fs: FileSystem, s: FileStatus, fileFilter: FileStatus => Boolean): List[FileStatus] = {

    walkFiles(fs, s, fileFilter, List[FileStatus]())
  }


  def walkFiles(fs: FileSystem, s: FileStatus, fileFilter: FileStatus => Boolean, acc: List[FileStatus]): List[FileStatus] =  s match {

    case _ if !fs.exists(s.getPath) =>
      acc
    case _ if s.isFile && fileFilter(s) =>
      s :: acc
    case _ if s.isFile =>
      acc
    case _ if s.isDirectory =>

      val statuses = fs.listStatus(s.getPath)

      statuses.foldLeft(acc) ((acc1, status) => {
          walkFiles(fs, status, fileFilter, acc1)
      })

    case _ =>
      acc
  }


  def subDirs(fs: FileSystem, s: FileStatus, dirFilter: FileStatus => Boolean): List[FileStatus] = (fs, s) match {

    case x if x._1.exists(x._2.getPath) && x._2.isDirectory => {

      val statuses = fs.listStatus(s.getPath)

      statuses.foldLeft(List[FileStatus]()) ((acc, status) => {

        status match {
          case _ if status.isDirectory && dirFilter(status) =>
            status :: acc
          case _ =>
            acc
        }
      })
    }
    case _ =>
      List[FileStatus]()
  }


  val fsParquetFilter: FileStatus => Boolean = gather => gather match {

    case f if f.getPath.toString.endsWith(".parquet") =>
      true
    case _ =>
      false
  }

  val fsLongFilter: FileStatus => Boolean = file => file match {

    case f if f.isFile =>

      val name = f.getPath.toString.split("/").last

      try {

        val long_string: Long = name.toLong

        //        debug(s"successfully parsed: $name")
        true
      }
      catch {
        case e =>
          //          debug(s"couldn't parse $name")
          false
      }

    case _ =>
      false
  }

  val fsDirTimestampFilter: FileStatus => Boolean = file => file match {

    case f if f.isDirectory =>

      try {

        val name = f.getPath.toString.split("/").last
        val date: DateTime = DateTime.parse(name, dateFormatter)
        val long_string: Long = date.getMillis

//        debug(s"successfully parsed: $name")
        true
      }
      catch {
        case e =>
//          debug(s"couldn't parse $name")
          false
      }

    case _ =>
      false
  }


  def getDeterminingStatus(statuses: Array[FileStatus]): FileStatus = {

    val offsetStatuses =  statuses.foldLeft(List[FileStatus]())((acc, status) => {

      status match {
        case s if s.isFile =>

          fsLongFilter(s) match {
            case true =>
              s :: acc
            case _ =>
              return s
          }

        case _ =>
          acc
      }
    })

    offsetStatuses.head
  }


  def getOrCreateOffset(fs: FileSystem, base: String, domain: String): OffsetState = {

    val offsetDirPath = s"$base/$OFFSET/$domain"

    val p = new Path(offsetDirPath)

    if(!fs.exists(p) || fs.listStatus(p).size == 0){

      createTextFile(fs, s"$offsetDirPath/0", "text")
    }

    val offsetStatus = getDeterminingStatus(fs.listStatus(p))

    val offsetFilePath = offsetStatus
      .getPath
      .toString

    val offsetFile = offsetFilePath
      .split("/")
      .last

    val offset = offsetFile match {
      case f if f.contains(LOCKED_SUFFIX) =>

        logInfo(s"Domain is locked:  $offsetFilePath")
        (true, f.replace(s"$LOCKED_SUFFIX", "").toLong)
      case _ =>
        (false, offsetFile.toLong)
    }

    logInfo(s"Offset: $offset")

    OffsetState(offset._1, offset._2, offsetStatus, offsetDirPath)
  }


  def lockOffsets(fs: FileSystem, oldOffset: FileStatus, potentialOffset: String): (Path, Path) = {

    val oldPath = oldOffset.getPath
    val oldLocked = new Path(s"${oldPath.toString}$LOCKED_SUFFIX")
    fs.rename(oldPath, oldLocked)

    val newLocked = createTextFile(fs, s"${potentialOffset}$LOCKED_SUFFIX", "text")

    (oldLocked, newLocked)
  }

  def unlockOffset(fs: FileSystem, oldPath: Path, lockedPath: Path) = {

    val unlockedPath = new Path(s"${lockedPath.toString}".replace(LOCKED_SUFFIX, ""))
    fs.rename(lockedPath, unlockedPath)
    fs.delete(oldPath, true)
  }


  def createTextFile(fs: FileSystem, fullPath: String, text: String): Path = {

    val path = new Path(fullPath)
    val out = fs.create(path)

    out.write(text.getBytes)
    out.flush()
    out.close()

    path
  }
}
