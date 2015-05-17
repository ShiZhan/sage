package algorithms

abstract class Algorithm[T](prefix: String, nShard: Int, reverse: Boolean, verticesDB: String) {
  import graph.{ Vertices, SimpleShards, DoubleShards }

  val shards = if (reverse) new DoubleShards(prefix, nShard) else new SimpleShards(prefix, nShard)
  val vertices = new Vertices[T](verticesDB)

  def iterations: Option[Iterator[String]]

  def run: Option[Iterator[String]] =
    if (shards.intact) {
      iterations
    } else {
      println("edge list(s) incomplete")
      None
    }
}