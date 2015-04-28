package graph

class Importer(edgeFN: String) extends helper.Logging {
  import EdgeUtils.invalidEdge
  import helper.Gauge.IteratorOperations

  private def line2edges(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line)
      invalidEdge
    case from :: to :: Nil => Edge(from.toLong, to.toLong)
    case _ =>
      logger.error("invalid: [{}]", line)
      invalidEdge
  }

  val edgesRaw =
    if (edgeFN.isEmpty())
      io.Source.fromInputStream(System.in).getLines()
    else
      io.Source.fromFile(edgeFN).getLines()
  val edges = edgesRaw.map(line2edges).filter(_.valid)

  def shardID(vertex: Long, nShard: Int) = (vertex & (nShard - 1)).toInt

  def run(nShard: Int) = {
    val shards = Shards.init(edgeFN, nShard)
    edges.foreachDo { e => shards(shardID(e.u, nShard)).putEdge(e) }
    shards.foreach(_.close)
  }
}

object Importer {
  def apply(edgeFN: String) = new Importer(edgeFN)
}