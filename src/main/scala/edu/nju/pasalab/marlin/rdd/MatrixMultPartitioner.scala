package edu.nju.pasalab.marlin.rdd

import edu.nju.pasalab.marlin.matrix.BlockID
import org.apache.spark.Partitioner

private[marlin] class MatrixMultPartitioner(
    val mSplitNum: Int,
    val kSplitNum: Int,
    val nSplitNum: Int) extends Partitioner{

  override def numPartitions: Int = mSplitNum * kSplitNum * nSplitNum

  override def getPartition(key: Any): Int = {
    key match {
        // actually, seq range: 0 ~ m*k*n - 1
      case (blockId: BlockID) =>
        blockId.seq
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case p: MatrixMultPartitioner =>
        (this.mSplitNum == p.mSplitNum) && (this.kSplitNum == p.kSplitNum) &&
          (this.nSplitNum == p.nSplitNum)
      case _ =>
        false
    }
  }
}
