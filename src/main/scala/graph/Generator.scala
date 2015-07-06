package graph

/**
 * @author Zhan
 * Output Synthetic Graph as edge list
 */
object Generator {
  import generators.GeneratorFactory
  import graph.EdgeUtils.edge2wedge

  def run(genOpt: String, edgeFileName: String, binary: Boolean, weight: Boolean) = {
    val edges = GeneratorFactory.getGenerator(genOpt).getEdges
    if (weight) {
      val wEdgeStorage =
        if (edgeFileName.isEmpty) WEdges.fromConsole
        else if (binary) WEdges.fromFile(edgeFileName) else WEdges.fromText(edgeFileName)
      wEdgeStorage.putEdges(edges.map(edge2wedge))
    } else {
      val edgeStorage =
        if (edgeFileName.isEmpty) Edges.fromConsole
        else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
      edgeStorage.putEdges(edges)
    }
  }
}