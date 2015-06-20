package edu.nju.pasalab.marlin.examples

/**
 * Created by v-yunta on 2015-05-11.
 */
object TestFunc {
  def main(args: Array[String]) {
//    val iter = Iterator(1, 2, 3, 4, 5)
//    println(s"result: ${func(iter).mkString(",")} ")
    val id = "join_broadcast_0_keyBlockID(1,1,2)"
    apply(id)

  }

  def func(iter: Iterator[Int]): Iterator[Double] = {
    Iterator.single(iter.sum.toDouble)
  }

  val JOINBROADCAST = "join_broadcast_([0-9]+)([_A-Za-z0-9.,()\\@]*)".r

  def apply(id: String) = id match {
    case JOINBROADCAST(broadcastId, field) =>
      JoinBroadcastBlockId(broadcastId.toLong, field.stripPrefix("_")); println("okay")
    case _ => throw new IllegalStateException("Unrecognized BlockId: " + id)
  }

  case class JoinBroadcastBlockId(broadcastId: Long, field: String = "") {
    def name = "join_broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
  }
}
