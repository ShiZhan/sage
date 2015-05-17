package algorithms

import graph.{ Edge, Vertices, Shards }

class PageRank(prefix: String, nShard: Int)
  extends Algorithm[Double](prefix, nShard, false, "") {
  def iterations = {
    Some(vertices.result)
  }
}
