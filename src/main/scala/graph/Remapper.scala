package graph

class Remapper(mapFile: String) {
  import helper.Lines
  import Lines.Lines2File

  private var index = -1L
  private val vertexMap =
    Lines.fromFile(mapFile).map { v => index += 1; (v.toLong, index) }.toMap

  private def mapEdge(e: Edge) =
    e match { case Edge(u, v) => Edge(vertexMap.getOrElse(u, u), vertexMap.getOrElse(v, v)) }

  def remap(edgeFile: String, outFile: String) =
    EdgeUtils.fromFile(edgeFile).map(mapEdge).toFile(outFile)
}

object Remapper {
  def run(edgeFile: String, mapFile: String, outFile: String) =
    new Remapper(mapFile).remap(edgeFile, outFile)
}