package graph

class Remapper(mapFile: String) {
  import helper.GetLines

  private var index = -1L
  private val vertexMap =
    GetLines.fromFile(mapFile).map { v => index += 1; (v.toLong, index) }.toMap

  private def mapEdge(e: Edge) =
    e match { case Edge(u, v) => Edge(vertexMap.getOrElse(u, u), vertexMap.getOrElse(v, v)) }

  def remap(edgeFile: String) =
    EdgeUtils.fromFile(edgeFile).map(mapEdge).foreach(println)
}

object Remapper {
  def run(edgeFile: String, mapFile: String) = new Remapper(mapFile).remap(edgeFile)
}