package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class KCore(prefix: String, nShard: Int)
  extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._

  def iterations = {
    println("Preparing vertex degree ...")
    val out = vertices.out
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        Seq(u, v).foreach { k =>
          if (out.containsKey(k)) {
            val d = out.get(k)
            out.put(k, d + 1)
          } else {
            out.put(k, 1)
          }
        }
    }

    println("Deducing K-Core ...")
    val data = vertices.data
    var core = 1L
    while (!out.isEmpty) {
      println(core)
      if (out.find { case (k, v) => v <= core } == None) core += 1
      else {
        //        for (Edge(u, v) <- shards.getAllEdges) {
        shards.getAllEdges.foreachDo {
          case Edge(u, v) =>
            if (out.get(u) <= core && out.containsKey(u) && out.containsKey(v)) {
              val c = out.get(v)
              out.put(v, c - 1)
              out.remove(u)
              data.put(u, core)
            } else if (out.get(v) <= core && out.containsKey(u) && out.containsKey(v)) {
              val c = out.get(u)
              out.put(u, c - 1)
              out.remove(v)
              data.put(v, core)
            }
        }
      }
    }

    println("Generating results ...")
    Some(vertices.result)
  }
}
