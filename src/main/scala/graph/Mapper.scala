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
  import helper.IteratorOps.ClosableIteratorWrapper

  private var index = -1L
  val id = Iterator.continually { index += 1; index }
  val mapFile = new File(mapFileName)
  val squeeze = !mapFile.exists
  val vMap = Map[Long, Long]()
  if (!squeeze) for (v <- Lines.fromFile(mapFile)) vMap.put(v.toLong, id.next)
  def storeMap = if (squeeze) vMap.toArray.sortBy(_._2).map(_._1).toIterator.toFile(mapFileName)

  def mapEdge(e: SimpleEdge) = e match {
    case Edge(u, v) if squeeze =>
      Edge(vMap.getOrElseUpdate(u, id.next), vMap.getOrElseUpdate(v, id.next))
    case Edge(u, v) if !squeeze =>
      Edge(vMap.getOrElse(u, u), vMap.getOrElse(v, v))
  }

  def map(edgeFileName: String, binary: Boolean) = {
    val edgeProvider =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges = edgeProvider.getEdges
    val mappedEdges = edges.map(mapEdge).atLast { () => storeMap }
    val outFileName = s"mapped-$edgeFileName"
    val edgeStorage =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(outFileName) else Edges.fromText(outFileName)
    edgeStorage.putEdges(mappedEdges)
  }
}

object Mapper {
  def apply(mapFileName: String) = new Mapper(mapFileName)
}