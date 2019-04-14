package com.hamshif.wielder.wild

import org.apache.hadoop.fs.FileStatus

/**
  *
  * @Author Gideon Bar
  */
case class OffsetState(
                        isLocked: Boolean,
                        offset: Long,
                        fileStatus: FileStatus,
                        basePath: String
                      )
