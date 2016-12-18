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

import java.io.Serializable
import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.storage.{JoinBroadcastBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer


class JoinBroadcast[K: ClassTag, D: ClassTag](rdd: RDD[(K, D)], val id: Long)
  extends Serializable with Logging{

  private val executorBroadcastId = JoinBroadcastBlockId(id)

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  //  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  //  @transient private var blockSize: Int =
  //    SparkEnv.get.conf.getInt("spark.broadcast.blockSize", 4096) * 1024
  //
  private def setConf(conf: SparkConf) {
    logInfo(s"start set conf in joinroadcast")
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
  }
  setConf(SparkEnv.get.conf)

  private val numBlocksMap = writeBlocksMap(rdd)

  def writeBlocksMap(rdd: RDD[(K, D)]): Map[K, Int] = {
    rdd.map { case (key, data) =>
      //      logInfo(s"join broadcast data to String: ${data.toString}")
      val compressionCodec: Option[CompressionCodec] =
        Some(CompressionCodec.createCodec(SparkEnv.get.conf))
      logInfo(s"join broadcast compressionCodec: ${compressionCodec.toString}")
      val blockSize: Int =
        SparkEnv.get.conf.getInt("spark.broadcast.blockSize", 4096) * 1024
      logInfo(s"join broadcast blockSize: $blockSize")

      //      logInfo(s"join broadcast default compress: ${
      //        SparkEnv.get.conf.getBoolean("spark.broadcast.compress", true)}")

      val blocks = TorrentBroadcast.blockifyObject(data,
        blockSize, SparkEnv.get.serializer, compressionCodec)
      blocks.zipWithIndex.foreach { case (block, i) =>
        SparkEnv.get.blockManager.putBytes[D](
          JoinBroadcastBlockId(id, "key" + key + "piece" + i),
          new ChunkedByteBuffer(block),
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true)
      }
      (key, blocks.length)
    }.collect().toMap
    //      collectAsMap().asInstanceOf[Map[K, Int]]
  }

  def destroy(blocking: Boolean = true) {
    JoinBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  def getValue(key: K): D = {
    logInfo(s"start get value")
    val numBlocks = numBlocksMap.get(key)
    assert(numBlocks.isDefined)
    val blocks = new Array[ByteBuffer](numBlocks.get)
    val bm = SparkEnv.get.blockManager

    //    TorrentBroadcast.synchronized {
    for (pid <- Random.shuffle(Seq.range(0, numBlocks.get))) {
      val pieceId = JoinBroadcastBlockId(id, "key" + key + "piece" + pid)

      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      var blockOpt = bm.getLocalBytes(pieceId)

      if (!blockOpt.isDefined) {
        logInfo(s"get joinBroadcast block from remote node")
        blockOpt = bm.getRemoteBytes(pieceId)
        blockOpt match {
          case Some(block) =>
            // If we found the block from remote executors/driver's BlockManager, put the block
            // in this executor's BlockManager.
            SparkEnv.get.blockManager.putBytes[D](
              pieceId,
              block,
              StorageLevel.MEMORY_AND_DISK_SER,
              tellMaster = true)

          case None =>
            throw new SparkException("Failed to get " + pieceId + " of " + executorBroadcastId)
        }
      } else {
        logInfo(s"get joinBroadcast block local")
      }
      // If we get here, the option is defined.
      blocks(pid) = blockOpt.get.toByteBuffer
    }
    //      logInfo(s"get join broadcast compressionCodec: ${compressionCodec.toString}")
    val value = TorrentBroadcast.unBlockifyObject[D](blocks,
      SparkEnv.get.serializer, Some(CompressionCodec.createCodec(SparkEnv.get.conf)))
    SparkEnv.get.broadcastManager.cacheJoinBroadcast(
      JoinBroadcastBlockId(id, "key" + key), value)
    value
    //    }
  }
}

private object JoinBroadcast {
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    SparkEnv.get.blockManager.master.removeJoinBroadcast(id, removeFromDriver, blocking)
    SparkEnv.get.broadcastManager.unCacheJoinBroadCast(id)
  }
}
