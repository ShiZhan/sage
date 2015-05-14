package algorithms

import graph.{ Edge, Vertices, Shards }

class Cluster(shards: Shards) {
  val vertices = Vertices[Long]

  def run = {
    vertices.result
  }
}
