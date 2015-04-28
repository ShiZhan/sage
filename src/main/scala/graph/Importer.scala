package graph

class Importer(edgeFN: String) extends helper.Logging {
  import helper.Gauge.IteratorOperations

  private def line2edges(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line)
      Edge()
    case from :: to :: Nil => Edge(from.toLong, to.toLong)
    case _ =>
      logger.error("invalid: [{}]", line)
      Edge()
  }

  def run(nShard: Int) = {
    val edgesRaw =
      if (edgeFN.isEmpty())
        io.Source.fromInputStream(System.in).getLines()
      else
        io.Source.fromFile(edgeFN).getLines()
    val edges = edgesRaw.map(line2edges).filter(_.valid)
    val shards = Shards.init(edgeFN, nShard)
    edges.foreachDo { e => shards(e.shardID(nShard)).putEdge(e) }
    shards.foreach(_.close)
  }
}

object Importer {
  def apply() = new Importer("")
  def apply(ifn: String) = new Importer(ifn)
}