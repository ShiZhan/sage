package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class KCore(shards: Shards) {
  import scala.collection.JavaConversions._

  val vertices = Vertices[Long]

  def run = {
    println("Preparing vertex degree ...")
    val out = vertices.out
    shards.getAllEdges.foreachDo { case Edge(u, v) =>
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
      core += 1
      while (out.values().find(_ < core) != None) {
        
      }
      data.putAll(out)
    }

    vertices.result
  }
}
