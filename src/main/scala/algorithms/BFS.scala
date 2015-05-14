/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

import graph.{ Edge, Vertices, Shards }

class BFS(shards: Shards) {
  val vertices = Vertices[Long]

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
      for (Edge(u, v) <- edges if (in.containsKey(u) && !data.containsKey(v))) {
        out.put(v, level)
        shards.setFlagByVertex(v)
      }
      vertices.update
    }
    vertices.result
  }
}

class BFS_U(shards: Shards) {
  val vertices = Vertices[Long]

  def run(root: Long) = {
    var level = 1L
    vertices.out.put(root, level)
    vertices.update

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getAllEdges
      val in = vertices.in
      val out = vertices.out

      level += 1L
      for (Edge(u, v) <- edges) {
        if (in.containsKey(u) && !data.containsKey(v)) out.put(v, level)
        if (in.containsKey(v) && !data.containsKey(u)) out.put(u, level)
      }
      vertices.update
    }
    vertices.result
  }
}

class BFS_P(shards: Shards) {
  import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }

  val vertices = new Vertices[Long]("")
  val system = ActorSystem("CoreSystem")

  def run(root: Long) = {
    var level = 1L
    vertices.out.put(root, level)
    shards.setFlagByVertex(root)
    vertices.update

    val data = vertices.data

    vertices.result
  }
}