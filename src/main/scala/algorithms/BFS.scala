/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

class BFS(prefix: String, nShard: Int, root: Long)
  extends Algorithm[Long](prefix, nShard, false, "") {
  import graph.Edge

  def iterations = {
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
    Some(vertices.result)
  }
}

class BFS_U(prefix: String, nShard: Int, root: Long)
  extends Algorithm[Long](prefix, nShard, false, "") {
  import graph.Edge

  def iterations = {
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
    Some(vertices.result)
  }
}

class BFS_P(prefix: String, nShard: Int, root: Long)
  extends Algorithm[Long](prefix, nShard, false, "") {
  import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }

  val system = ActorSystem("CoreSystem")

  def iterations = {
    Some(vertices.result)
  }
}