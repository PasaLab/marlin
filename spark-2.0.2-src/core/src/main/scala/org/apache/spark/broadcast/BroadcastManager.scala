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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BroadcastBlockId, JoinBroadcastBlockId, StorageLevel}
import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.SparkEnv

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null
  private var broadcastMap: HashMap[BroadcastBlockId, Any] = new HashMap[BroadcastBlockId, Any]()
  private val cacheInBroadcastManager =
    conf.getBoolean("spark.broadcast.CacheInBroadcastManager", true)
  private var jBroadcastMap: HashMap[JoinBroadcastBlockId, Any] =
    new HashMap[JoinBroadcastBlockId, Any]()

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
  def newJoinBroadcast[K: ClassTag, D: ClassTag](rdd: RDD[(K, D)]): JoinBroadcast[K, D] = {
    assert(broadcastFactory.isInstanceOf[TorrentBroadcastFactory]) // TODO: if HTTP?
    logInfo(s"broadcastFactory start new JoinBroadcast")
    broadcastFactory.asInstanceOf[TorrentBroadcastFactory].
      newJoinBroadcast[K, D](rdd, nextBroadcastId.getAndIncrement())
  }

  def newExecutorBroadcast[D: ClassTag](rdd: RDD[D]): ExecutorBroadcast[D] = {
    assert(broadcastFactory.isInstanceOf[TorrentBroadcastFactory]) // TODO: if HTTP?
    logInfo(s"broadcastFactory start new ExecutorBroadcast")
    broadcastFactory.asInstanceOf[TorrentBroadcastFactory].
      newExecutorBroadcast[D](rdd, nextBroadcastId.getAndIncrement())
  }
  def joinUnbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    assert(broadcastFactory.isInstanceOf[TorrentBroadcastFactory]) // TODO: if HTTP?
    broadcastFactory.asInstanceOf[TorrentBroadcastFactory].
      joinUnbroadcast(id, removeFromDriver, blocking)
  }

  def unExecutorBroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    assert(broadcastFactory.isInstanceOf[TorrentBroadcastFactory]) // TODO: if HTTP?
    broadcastFactory.asInstanceOf[TorrentBroadcastFactory].
      unbroadcast(id, removeFromDriver, blocking)
  }

  def getBroadcastValue(id: BroadcastBlockId): Option[Any] = {
    if (cacheInBroadcastManager) {
      logInfo(s"Get broadcast $id from BroadcastManager.")
      broadcastMap.get(id)
    } else {
      logInfo(s"Get broadcast $id from BlockManager.")
      SparkEnv.get.blockManager.getLocalValues(id).map(_.data.next())
    }
  }

  def cacheBroadcast(id: BroadcastBlockId, value: Any): Unit = {
    if (cacheInBroadcastManager) {
      logInfo(s"Store broadcast $id in BroadcastManager.")
      broadcastMap.put(id, value)
    } else {
      logInfo(s"Store broadcast $id in BlockManager.")
      SparkEnv.get.blockManager.putSingle(id, value,
        StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    }
  }

  def unCacheBroadCast(id: BroadcastBlockId): Unit = {
    if (cacheInBroadcastManager) {
      logInfo(s"Remove broadcast $id from BroadcastManager.")
      broadcastMap.remove(id)
    } else {
      logInfo(s"Remove broadcast $id from BlockManager.")
      SparkEnv.get.blockManager.removeBlock(id, true)
    }
  }

  def getJoinBroadcastValue(id: JoinBroadcastBlockId): Option[Any] = {
    jBroadcastMap.get(id)
  }

  def cacheJoinBroadcast(id: JoinBroadcastBlockId, value: Any) {
    jBroadcastMap.put(id, value)
  }

  def unCacheJoinBroadCast(id: Long) {
    jBroadcastMap = jBroadcastMap.filterNot(_._1.name.startsWith("join_broadcast_" + id))
  }
}
