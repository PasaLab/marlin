package edu.nju.pasalab.marlin.examples

/**
 * Created by v-yunta on 2015-05-11.
 */
object TestFunc {
  def main(args: Array[String]) {
    val iter = Iterator(1, 2, 3, 4, 5)
    println(s"result: ${func(iter).mkString(",")} ")
  }

  def func(iter: Iterator[Int]): Iterator[Double] = {
    Iterator.single(iter.sum.toDouble)
  }
}
