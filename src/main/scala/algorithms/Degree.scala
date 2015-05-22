package algorithms

case class DirectedDegree(i: Long, o: Long) {
  override def toString = s"$i $o"
}

class Degree(prefix: String, nShard: Int)
    extends Algorithm[DirectedDegree](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._
  import helper.Gauge.IteratorOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        val DirectedDegree(uI, uO) = data.getOrElse(u, DirectedDegree(0, 0))
        val DirectedDegree(vI, vO) = data.getOrElse(v, DirectedDegree(0, 0))
        data.put(u, DirectedDegree(uI, uO + 1))
        data.put(v, DirectedDegree(vI + 1, vO))
    }
  }
}

class Degree_U(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._
  import helper.Gauge.IteratorOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        val uD: Long = data.getOrElse(u, 0); data.put(u, uD + 1)
        val vD: Long = data.getOrElse(v, 0); data.put(v, vD + 1)
    }
  }
}