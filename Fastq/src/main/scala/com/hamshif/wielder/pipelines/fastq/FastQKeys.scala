package com.hamshif.wielder.pipelines.fastq

trait FastQKeys {

  val KEY_FASTQ = "fastq"

  val KEY_SEQUENCE_IDENTIFIER = "sequence_identifier"
  val KEY_SEQUENCE = "sequence"
  val KEY_QUALITY_SCORE_IDENTIFIER = "quality_score_identifier"
  val KEY_QUALITY_SCORE = "quality"

  val KEY_UNIQUE = "unique"

  val KEY_SHORT = "s_"

  val KEY_S_SEQUENCE_IDENTIFIER = s"$KEY_SHORT$KEY_SEQUENCE_IDENTIFIER"
  val KEY_S_SEQUENCE = s"$KEY_SHORT$KEY_SEQUENCE"
  val KEY_S_QUALITY_SCORE_IDENTIFIER = s"$KEY_SHORT$KEY_QUALITY_SCORE_IDENTIFIER"
  val KEY_S_QUALITY_SCORE = s"$KEY_SHORT$KEY_QUALITY_SCORE"
  val KEY_S_UNIQUE = s"$KEY_SHORT$KEY_UNIQUE"

  val KEY_UMI = "umi"
  val KEY_BARCODE = "barcode"
  val KEY_MIN_READ = "min_read"
  val KEY_MIN_READ_BARCODE = "min_read_barcode"
  val KEY_ACC_QUALITY_SCORE = s"accumulated_quality"

  val KEY_FILTERED_DUPLICATES = "filtered_duplicates"
  val KEY_FILTERED_SIMILAR = "filtered_similar"

  val SEQUENCE_DF_FIELDS = Array[String](
    KEY_SEQUENCE_IDENTIFIER,
    KEY_SEQUENCE,
    KEY_QUALITY_SCORE_IDENTIFIER,
    KEY_QUALITY_SCORE
  )

  val BARCODE_DF_FIELDS = Array[String](
    KEY_S_SEQUENCE_IDENTIFIER,
    KEY_S_SEQUENCE,
    KEY_S_QUALITY_SCORE_IDENTIFIER,
    KEY_S_QUALITY_SCORE
  )

  val DERIVED = Array[String](
    KEY_UNIQUE,
    KEY_UMI,
    KEY_BARCODE,
    KEY_MIN_READ,
    KEY_ACC_QUALITY_SCORE
  )

  val COMBINED = BARCODE_DF_FIELDS ++ SEQUENCE_DF_FIELDS ++ DERIVED
}
