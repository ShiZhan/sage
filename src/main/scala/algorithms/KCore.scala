package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class KCore(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._

  def iterations = {
    logger.info("Preparing vertex degree ...")
    val s0 = scatter
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        Seq(u, v).foreach { k =>
          if (s0.containsKey(k)) {
            val d = s0.get(k)
            s0.put(k, d + 1)
          } else {
            s0.put(k, 1)
          }
        }
    }

    logger.info("Deducing K-Core ...")
    var core = 1L
    while (!s0.isEmpty) {
      logger.info("gathering core: [{}]", core)
      if (s0.find { case (k, v) => v <= core } == None) core += 1
      else
        for (Edge(u, v) <- shards.getAllEdges) {
          if (s0.containsKey(u) && s0.containsKey(v))
            if (s0.get(u) <= core) {
              val c = s0.get(v)
              s0.put(v, c - 1)
              s0.remove(u)
              data.put(u, core)
            }
          if (s0.containsKey(u) && s0.containsKey(v))
            if (s0.get(v) <= core) {
              val c = s0.get(u)
              s0.put(u, c - 1)
              s0.remove(v)
              data.put(v, core)
            }
        }
    }
  }
}
