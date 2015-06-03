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
  private def id = Iterator.continually { index += 1; index }
  private val vertexMap = Lines.fromFile(mapFile).map { v => (v.toLong, id.next) }.toMap

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

class Squeezer {
  import scala.collection.mutable.Map
  import helper.Lines
  import Lines.LinesWrapper
  import Edges.EdgesWrapper

  private var index = -1L
  private def id = Iterator.continually { index += 1; index }
  private val vertexMap = Map[Long, Long]()

  private def mapEdge(e: Edge) = e match {
    case Edge(u, v) =>
      Edge(vertexMap.getOrElseUpdate(u, id.next), vertexMap.getOrElseUpdate(v, id.next))
  }

  def remapT(edgeFile: String, outFile: String) =
    Edges.fromLines(edgeFile).map(mapEdge).toText(outFile)

  def remapB(edgeFile: String, outFile: String) =
    if (!(edgeFile.isEmpty || outFile.isEmpty))
      EdgeFile(edgeFile).getThenClose.map(mapEdge).toFile(outFile)

  def storeMap(mapFile: String) =
    vertexMap.toArray.map { case (k, v) => Edge(v, k) }.sorted.map(_.v).toIterator.toFile(mapFile)
}

object Remapper {
  def run(edgeFile: String, mapFile: String, outFile: String, binary: Boolean) = mapFile match {
    case "squeeze" =>
      val m = new Squeezer
      if (binary) m.remapB(edgeFile, outFile) else m.remapT(edgeFile, outFile)
      m.storeMap(edgeFile match { case "" => "squeeze.map"; case f => f + ".map" })
    case mFileName =>
      val m = new Remapper(mFileName)
      if (binary) m.remapB(edgeFile, outFile) else m.remapT(edgeFile, outFile)
  }
}