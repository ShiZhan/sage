package graph

/**
 * @author Zhan
 * edge list remapper
 * remapT: text mode remapper
 * remapB: binary mode remapper
 */
class Remapper(mapFile: String) {
  import helper.Lines
  import Edges.EdgesWrapper

  private var index = -1L
  private val vertexMap =
    Lines.fromFile(mapFile).map { v => index += 1; (v.toLong, index) }.toMap

  private def mapEdge(e: Edge) = e match {
    case Edge(u, v) =>
      Edge(vertexMap.getOrElse(u, u), vertexMap.getOrElse(v, v))
  }

  def remapT(edgeFile: String, outFile: String) =
    Edges.fromLines(edgeFile).map(mapEdge).toText(outFile)

  def remapB(edgeFile: String, outFile: String) =
    if (!(edgeFile.isEmpty || outFile.isEmpty))
      EdgeFile(edgeFile).getThenClose.map(mapEdge).toFile(outFile)
}

object Remapper {
  def run(edgeFile: String, mapFile: String, outFile: String, binary: Boolean) = {
    val m = new Remapper(mapFile)
    if (binary) m.remapB(edgeFile, outFile) else m.remapT(edgeFile, outFile)
  }
}