package edu.nju.pasalab.marlin.rdd

import edu.nju.pasalab.marlin.matrix.BlockID
import org.apache.spark.{Logging, Partitioner}

private[marlin] class MatrixMultPartitioner(
    val mSplitNum: Int,
    val kSplitNum: Int,
    val nSplitNum: Int) extends Partitioner with Logging{

//  override def numPartitions: Int = mSplitNum * nSplitNum
  override def numPartitions: Int = mSplitNum * kSplitNum * nSplitNum

  override def getPartition(key: Any): Int = {
    key match {
      case (blockId: BlockID) =>
//        blockId.row * mSplitNum + blockId.column
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
