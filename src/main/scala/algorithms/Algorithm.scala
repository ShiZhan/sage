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
    val sgStat = s"[ % 10d -> % 10d ] Flagged: % 5d (% 4d%% )"
      .format(gather.size, scatter.size, shards.getFlagedTotal, 100 * shards.getFlagedTotal / nShard)
    gather.clear()
    data.putAll(scatter)
    stepCounter += 1
    logger.info("Step {}: {}", stepCounter, sgStat)
  }

  def iterations: Unit

  def run =
    if (shards.intact) {
      logger.info("Data:         [{}]", prefix)
      logger.info("Sharding:     [{}]", nShard)
      logger.info("Reverse edge: [{}]", reverse)
      logger.info("Vertex DB:    [{}]", verticesDB)
      iterations
      if (data.isEmpty())
        None
      else {
        logger.info("Generating results ...")
        val result = data.toIterator
        Some(result)
      }
    } else {
      logger.info("edge list(s) incomplete")
      None
    }
}