package graph

/**
 * @author Zhan
 * edge list mapper
 * mapFileName: map ID in "edgeFileName" by "mapFileName" to mapped file
 */
class Mapper(mapFileName: String) {
  import java.io.File
  import scala.collection.mutable.Map
  import helper.Lines
  import Lines.LinesWrapper

  val id = Iterator.iterate(0L)(_ + 1)
  val mapFile = new File(mapFileName)
  val squeeze = !mapFile.exists
  val vMap = Map[Long, Long]()
  if (!squeeze) for (v <- Lines.fromFile(mapFile)) vMap.put(v.toInt, id.next)

  val mapEdge: PartialFunction[SimpleEdge, SimpleEdge] = {
    case Edge(u, v) =>
      Edge(vMap.getOrElseUpdate(u, id.next), vMap.getOrElseUpdate(v, id.next))
  }

  def map(edgeFileName: String, binary: Boolean) = {
    val edgeProvider = if (edgeFileName.isEmpty) Edges.fromConsole
    else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges = edgeProvider.getEdges
    val mappedEdges = edges.map(mapEdge)
    val edgeStorage = if (edgeFileName.isEmpty) Edges.fromConsole
    else if (binary) Edges.fromFile(s"$edgeFileName-mapped.bin")
    else Edges.fromText(s"$edgeFileName-mapped.edges")
    edgeStorage.putEdges(mappedEdges)
    if (squeeze) vMap.toArray.sortBy(_._2).map(_._1).toIterator.toFile(mapFileName)
  }
}

object Mapper {
  def apply(mapFileName: String) = new Mapper(mapFileName)
}