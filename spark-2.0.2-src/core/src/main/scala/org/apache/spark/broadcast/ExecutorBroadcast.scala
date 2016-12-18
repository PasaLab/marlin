/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.broadcast

import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.storage.ExecutorBroadcastBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.util.io.ChunkedByteBuffer


class ExecutorBroadcast[D: ClassTag](rdd: RDD[D], id: Long)
  extends Broadcast[Array[D]](id) with Serializable with Logging{
  /**
    * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
    * which builds this value by reading blocks from the driver and/or other executors.
    *
    * On the driver, if the value is required, it is read lazily from the block manager.
    */
  @transient private lazy val _value: Array[D] = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _

  private val broadcastId = ExecutorBroadcastBlockId(id)

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
  }
  setConf(SparkEnv.get.conf)

  /** Total number of blocks this broadcast variable contains. */
  private val numBlocksWithIndex: Map[Int, Int] = writeBlocks(rdd)

  override protected def getValue() = {
    _value
  }

  /**
    * Divide the object into multiple blocks and put those blocks in the block manager.
    * @param rdd the rdd to divide, each block manager contains the data
    * @return the index of this partition and the number of blocks
    */
  private def writeBlocks(rdd: RDD[D]): Map[Int, Int] = {
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    rdd.mapPartitionsWithIndex {
      case (idx, iter) =>
        val compressionCodec: Option[CompressionCodec] =
          Some(CompressionCodec.createCodec(SparkEnv.get.conf))
        logInfo(s"executor broadcast compressionCodec: ${compressionCodec.toString}")
        val blockSize: Int =
          SparkEnv.get.conf.getInt("spark.broadcast.blockSize", 4096) * 1024
        logInfo(s"executor broadcast blockSize: $blockSize")
        val blocks = TorrentBroadcast.blockifyObject(iter.toArray,
          blockSize, SparkEnv.get.serializer, compressionCodec)
        blocks.zipWithIndex.foreach {
          case (block, i) =>
            SparkEnv.get.blockManager.putBytes(
              ExecutorBroadcastBlockId(id, "part" + idx + "piece" + i),
              new ChunkedByteBuffer(block),
              StorageLevel.MEMORY_AND_DISK_SER,
              tellMaster = true)
        }
        Iterator.single((idx, blocks.length))
    }.collect().toMap
  }

  /** Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(part: Int): Array[ByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[ByteBuffer](numBlocksWithIndex.get(part).get)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, blocks.length))) {
      val pieceId = ExecutorBroadcastBlockId(id, "part" + part + "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      def getLocal: Option[ByteBuffer] = bm.getLocalBytes(pieceId).map(_.toByteBuffer)
      def getRemote: Option[ByteBuffer] = bm.getRemoteBytes(pieceId).map { block =>
        // If we found the block from remote executors/driver's BlockManager, put the block
        // in this executor's BlockManager.
        SparkEnv.get.blockManager.putBytes(
          pieceId,
          block,
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true)
        block.toByteBuffer
      }
      val block: ByteBuffer = getLocal.orElse(getRemote).getOrElse(
        throw new SparkException(s"Failed to get $pieceId of $broadcastId"))
      blocks(pid) = block
    }
    blocks
  }

  private def readBroadcastBlock(): Array[D] = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)

      SparkEnv.get.blockManager.getLocalValues(broadcastId).map(_.data.next()) match {
        case Some(x) =>
          logInfo(s"the value exist some x: $x")
          x.asInstanceOf[Array[D]]

        case None =>
          logInfo(s"the value not exist")
          logInfo("Started reading broadcast variable " + id)
          var result = Array[D]()
          val startTimeMs = System.currentTimeMillis()
          val parts = numBlocksWithIndex.keys.toSeq
          for( part <- Random.shuffle(parts)) {
            val blocks = readBlocks(part)
            val obj = TorrentBroadcast.unBlockifyObject[D](
              blocks, SparkEnv.get.serializer, compressionCodec)
            result ++= obj.asInstanceOf[Array[D]]
          }
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          SparkEnv.get.blockManager.putSingle(
            broadcastId, result, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))
          result
      }
    }
  }

  /**
    * Actually destroy all data and metadata related to this broadcast variable.
    * Implementation of Broadcast class must define their own logic to destroy their own
    * state.
    */
  override protected def doDestroy(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /**
    * Actually unpersist the broadcasted value on the executors. Concrete implementations of
    * Broadcast class must define their own logic to unpersist their own data.
    */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }
}