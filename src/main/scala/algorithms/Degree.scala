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
        val DirectedDegree(uI, uO) = data.getOrDefault(u, DirectedDegree(0, 0))
        val DirectedDegree(vI, vO) = data.getOrDefault(v, DirectedDegree(0, 0))
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
        val uD = data.getOrDefault(u, 0); data.put(u, uD + 1)
        val vD = data.getOrDefault(v, 0); data.put(v, vD + 1)
    }
  }
}