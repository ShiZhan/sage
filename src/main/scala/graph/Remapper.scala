package graph

class Remapper(mapFile: String) {
  import helper.Lines
  import Edges.EdgesWrapper

  private var index = -1L
  private val vertexMap =
    Lines.fromFile(mapFile).map { v => index += 1; (v.toLong, index) }.toMap

  private def mapEdge(e: Edge) =
    e match { case Edge(u, v) => Edge(vertexMap.getOrElse(u, u), vertexMap.getOrElse(v, v)) }

  def remap(edgeFile: String, outFile: String, binary: Boolean) = if (binary) {
    if (!(edgeFile.isEmpty || outFile.isEmpty)) Edges.fromFile(edgeFile).all.map(mapEdge).toFile(outFile)
  } else
    Edges.fromLines(edgeFile).map(mapEdge).toText(outFile)
}

object Remapper {
  def run(edgeFile: String, mapFile: String, outFile: String, binary: Boolean) =
    new Remapper(mapFile).remap(edgeFile, outFile, binary)
}