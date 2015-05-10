package algorithms

import graph.{ Edge, Vertices, Shards }

class BFS(shards: Shards) {
  val vertices = new Vertices[Long]("")

  def run(root: Long) = {
    var level = 1L
    vertices.out.put(root, level)
    shards.setFlagByVertex(root)
    vertices.update

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getFlagedEdges
      val in = vertices.in
      val out = vertices.out
      level += 1L
      for (e <- edges) {
        val Edge(u, v) = e
        val valueU = in.get(u)
        if (valueU != 0L) {
          val valueV = data.get(v)
          if (valueV == 0L) {
            out.put(v, level)
            shards.setFlagByVertex(v)
          }
        }
      }
      vertices.update
    }
    vertices.print
  }
}

class ParallelBFS(shards: Shards) {
  import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }

  val vertices = new Vertices[Long]("")
  val system = ActorSystem("CoreSystem")

  def run(root: Long) = {
    var level = 1L
    vertices.out.put(root, level)
    shards.setFlagByVertex(root)
    vertices.update

    val data = vertices.data

    vertices.print
  }
}
