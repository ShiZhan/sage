package algorithms

abstract class Algorithm[T](prefix: String, nShard: Int, reverse: Boolean, verticesDB: String)
    extends helper.Logging {
  import scala.collection.JavaConversions._
  import graph.{ Vertices, SimpleShards, DoubleShards }

  val shards = if (reverse) new DoubleShards(prefix, nShard) else new SimpleShards(prefix, nShard)
  val vertices = new Vertices[T](verticesDB)
  var stepCounter = 0
  def step(i: Int) = vertices.getVertexTable(s"$i")
  val data = step(0)
  def gather = step(stepCounter)
  def scatter = step(stepCounter + 1)

  def update = {
    val gathered = gather.size()
    val scattered = scatter.size()
    gather.clear()
    data.putAll(scatter)
    stepCounter += 1
    logger.info("step [{}] (gather, scatter): [{}]", stepCounter, (gathered, scattered))
  }

  def iterations: Unit

  def run =
    if (shards.intact) {
      iterations
      if (data.isEmpty())
        None
      else {
        println("Generating results ...")
        val result = data.toIterator
        Some(result)
      }
    } else {
      println("edge list(s) incomplete")
      None
    }
}