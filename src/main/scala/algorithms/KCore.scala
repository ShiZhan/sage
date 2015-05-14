package algorithms

import graph.{ Edge, Vertices, Shards }
import helper.Gauge.IteratorOperations

class KCore(shards: Shards) {
  import scala.collection.JavaConversions._

  val vertices = Vertices[Long]

  def run = {
    val out0 = vertices.out
    println("Preparing vertex degree ...")
    shards.getAllEdges.foreachDo { case Edge(u, v) =>
      Seq(u, v).foreach { k =>
        if (out0.containsKey(k)) {
          val d = out0.get(k)
          out0.put(k, d + 1)
        } else {
          out0.put(k, 1)
        }
      }
    }
    vertices.update

    println("Deducing K-Core ...")
    var core = 1L
    while (!vertices.in.isEmpty) {
      core += 1
      val in = vertices.in
      val out = vertices.out
      while (in.values().find { _ < core } != None) {
        
      }
      out.putAll(in)
      vertices.update
    }

    vertices.print
  }
}
