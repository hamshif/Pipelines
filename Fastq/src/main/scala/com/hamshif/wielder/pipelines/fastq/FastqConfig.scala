package com.hamshif.wielder.pipelines.fastq

/**
  * @author Gideon Bar
  * @param minBases Minimum bases for read 2 (R2) to be considered similar
  * @param minFidelityScore Minimum transcription fidelity score for read 2 (R2) not to be filtered
  */
case class FastqConfig(
                        debugVerbose: Boolean = false,
                        minBases: Int = 5,
                        minFidelityScore: Int = 1000
                     )
