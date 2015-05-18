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
    scatter.put(root, level)
    shards.setFlagByVertex(root)
    update

    while (!gather.isEmpty) {
      val edges = shards.getFlagedEdges
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- edges if (g.containsKey(u) && !data.containsKey(v))) {
        s.put(v, level)
        shards.setFlagByVertex(v)
      }
      update
    }
  }
}

class BFS_U(prefix: String, nShard: Int, root: Long)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    update

    while (!gather.isEmpty) {
      val edges = shards.getAllEdges
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- edges) {
        if (g.containsKey(u) && !data.containsKey(v)) s.put(v, level)
        if (g.containsKey(v) && !data.containsKey(u)) s.put(u, level)
      }
      update
    }
  }
}

class BFS_P(prefix: String, nShard: Int, root: Long)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }

  val system = ActorSystem("CoreSystem")

  def iterations = {
  }
}