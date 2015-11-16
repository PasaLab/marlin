package edu.nju.pasalab.marlin.utils

import org.apache.spark.Logging
import org.scalatest.{Outcome, FunSuite}

private[marlin] abstract class MarlinFunSuite extends FunSuite with Logging{

  override protected def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName.replaceAll("edu.nju.pasalab.marlin", "marlin")
    try{
      logInfo(s"\n\n==== TEST OUTPUT FOR $suiteName: $testName ====\n")
      test()
    } finally {
      logInfo(s"\n==== FINISHED $suiteName: $testName ====\n")
    }
  }
}
