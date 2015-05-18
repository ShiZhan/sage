package algorithms

class Degree(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._
  import helper.Gauge.IteratorOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        Seq(u, v).foreach { k =>
          val d = data.get(k)
          data.put(k, d + 1)
        }
    }
  }
}