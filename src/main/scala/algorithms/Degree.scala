package algorithms

class Degree(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._
  import helper.Gauge.IteratorOperations
  import graph.Edge

  def iterations = {
    val data = vertices.data
    println("Counting vertex degree ...")
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        Seq(u, v).foreach { k =>
          if (data.containsKey(k)) {
            val d = data.get(k)
            data.put(k, d + 1)
          } else {
            data.put(k, 1)
          }
        }
    }

    println("Generating results ...")
    Some(vertices.result)
  }
}
