package graph

/**
 * @author Zhan
 * Output Synthetic Graph as edge list
 */
object Generator extends helper.Logging {
  import generators.{ GeneratorFactory, GeneratorUtils }
  import GeneratorUtils.WEdgeConverter

  def run(genOpt: String, edgeFileName: String, binary: Boolean, weight: Boolean) = {
    val edges = GeneratorFactory.getGenerator(genOpt).getEdges
    if (weight) {
      val wEdgeStorage =
        if (edgeFileName.isEmpty) WEdges.fromConsole
        else if (binary) WEdges.fromFile(edgeFileName) else WEdges.fromText(edgeFileName)
      logger.debug("START")
      wEdgeStorage.putEdges(edges.toWEdges)
      logger.debug("COMPLETE")
    } else {
      val edgeStorage =
        if (edgeFileName.isEmpty) Edges.fromConsole
        else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
      logger.debug("START")
      edgeStorage.putEdges(edges)
      logger.debug("COMPLETE")
    }
  }
}