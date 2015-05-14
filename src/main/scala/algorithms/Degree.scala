package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class Degree(shards: Shards) {
  import scala.collection.JavaConversions._

  val vertices = Vertices[Long]

  def run = {
    val data = vertices.data
    println("Counting vertex degree ...")
    shards.getAllEdges.foreachDo { case Edge(u, v) =>
      Seq(u, v).foreach { k =>
        if (data.containsKey(k)) {
          val d = data.get(k)
          data.put(k, d + 1)
        } else {
          data.put(k, 1)
        }
      }
    }
    vertices.print
  }
}
