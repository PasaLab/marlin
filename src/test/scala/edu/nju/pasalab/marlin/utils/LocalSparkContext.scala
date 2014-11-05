package edu.nju.pasalab.marlin.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Suite, BeforeAndAfterAll}


trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override protected def beforeAll {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override protected def afterAll(configMap: Map[String, Any]) {
    if (sc != null){
      sc.stop()
    }
    super.afterAll(configMap)
  }
}
