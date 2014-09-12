package edu.nju.pasalab.examples

import edu.nju.pasalab.sparkmatrix.MTUtils


object Carma {

  def main(args: Array[String]) {
    val result = MTUtils.splitMethod(15000, 15000, 15000, 256)
    println("the split method:" + result.toString())
  }

}
