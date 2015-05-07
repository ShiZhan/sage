package algorithms

import graph.{ Edge, Vertices, Shards }

case class VertexStat(degree: Int, core: Int)

class Stat(shards: Shards) {
  val vertices = new Vertices[VertexStat]("")

  def run = {
  }
}
