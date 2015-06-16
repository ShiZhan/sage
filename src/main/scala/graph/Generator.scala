package graph

/**
 * @author Zhan
 * Output Synthetic Graph as edge list
 */
object Generator extends helper.Logging {
  def run(genOpt: String, edgeFileName: String, binary: Boolean) = {
    val edges = generators.GeneratorFactory.optParser(genOpt).getEdges
    val edgeStorage =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    logger.debug("START")
    edgeStorage.putEdges(edges)
    logger.debug("COMPLETE")
  }
}