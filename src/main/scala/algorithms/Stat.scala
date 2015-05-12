package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class Stat(shards: Shards) {
  case class V(degree: Int, core: Int)
  val vertices = new Vertices[V]("")

  def run = {
    val data = vertices.data
    shards.getAllEdges.foreachDo { case Edge(u, v) =>
      Seq(u, v).foreach { k =>
        if (data.containsKey(k)) {
          val V(d, c) = data.get(k)
          data.put(k, V(d + 1, c))
        } else {
          data.put(k, V(1, 1))
        }
      }
    }
    vertices.print
  }
}
