package graph

class Graph[T](edgeFile: String, vdbFile: String) {
  val vdb = Vertices[T](vdbFile)
  def v(mapName: String) = vdb.getVertices(mapName)
  def e = Edges.fromFile(edgeFile)
}