package graph

/**
 * @author Zhan
 * edge list remapper
 * remapT: text mode remapper
 * remapB: binary mode remapper
 */
//class Remapper(mapFile: String) {
//  import helper.Lines
//  import Edges.EdgesWrapper
//
//  private var index = -1L
//  private def id = Iterator.continually { index += 1; index }
//  private val vertexMap = Lines.fromFile(mapFile).map { v => (v.toLong, id.next) }.toMap
//
//  private def mapEdge(e: Edge) = e match {
//    case Edge(u, v) =>
//      Edge(vertexMap.getOrElse(u, u), vertexMap.getOrElse(v, v))
//  }
//
//  def remapT(edgeFile: String, outFile: String) =
//    Edges.fromLines(edgeFile).map(mapEdge).toText(outFile)
//
//  def remapB(edgeFile: String, outFile: String) =
//    if (!(edgeFile.isEmpty || outFile.isEmpty))
//      EdgeFile(edgeFile).getThenClose.map(mapEdge).toFile(outFile)
//}
//
//class Squeezer {
//  import scala.collection.mutable.Map
//  import helper.Lines
//  import Lines.LinesWrapper
//  import Edges.EdgesWrapper
//
//  private var index = -1L
//  private def id = Iterator.continually { index += 1; index }
//  private val vertexMap = Map[Long, Long]()
//
//  private def mapEdge(e: Edge) = e match {
//    case Edge(u, v) =>
//      Edge(vertexMap.getOrElseUpdate(u, id.next), vertexMap.getOrElseUpdate(v, id.next))
//  }
//
//  def remapT(edgeFile: String, outFile: String) =
//    Edges.fromLines(edgeFile).map(mapEdge).toText(outFile)
//
//  def remapB(edgeFile: String, outFile: String) =
//    if (!(edgeFile.isEmpty || outFile.isEmpty))
//      EdgeFile(edgeFile).getThenClose.map(mapEdge).toFile(outFile)
//
//  def storeMap(mapFile: String) =
//    vertexMap.toArray.sortBy(_._2).map(_._1).toIterator.toFile(mapFile)
//}
//
//object Remapper {
//  def run(edgeFile: String, mapFile: String, outFile: String, binary: Boolean) = mapFile match {
//    case "squeeze" =>
//      val m = new Squeezer
//      if (binary) m.remapB(edgeFile, outFile) else m.remapT(edgeFile, outFile)
//      val sMapFile = if (edgeFile.isEmpty) "squeeze.map" else edgeFile + "-squeeze.map"
//      m.storeMap(sMapFile)
//    case mFileName =>
//      val m = new Remapper(mFileName)
//      if (binary) m.remapB(edgeFile, outFile) else m.remapT(edgeFile, outFile)
//  }
//}

class Mapper(mapFileName: String) {
  import java.io.File
  import scala.collection.mutable.Map
  import helper.Lines
  import Lines.LinesWrapper
  import Edges.EdgesWrapper
  import helper.IteratorOps.ClosableIteratorWrapper

  private var index = -1L
  val id = Iterator.continually { index += 1; index }
  val mapFile = new File(mapFileName)
  val squeeze = !mapFile.exists
  val vMap = Map[Long, Long]()
  if (!squeeze) for (v <- Lines.fromFile(mapFile)) vMap.put(v.toLong, id.next)
  def storeMap = if (squeeze) vMap.toArray.sortBy(_._2).map(_._1).toIterator.toFile(mapFileName)

  def mapEdge(e: Edge) = e match {
    case Edge(u, v) if squeeze =>
      Edge(vMap.getOrElseUpdate(u, id.next), vMap.getOrElseUpdate(v, id.next))
    case Edge(u, v) if !squeeze =>
      Edge(vMap.getOrElse(u, u), vMap.getOrElse(v, v))
  }

  def map(edgeFileName: String, binary: Boolean) = {
    val edges = if (binary) EdgeFile(edgeFileName).getThenClose else Edges.fromLines(edgeFileName)
    val mappedEdges = edges.map(mapEdge).atLast { () => storeMap }
    val outFileName = s"mapped-$edgeFileName"
    if (binary) mappedEdges.toFile(outFileName) else mappedEdges.toText(outFileName)
  }
}

object Mapper {
  def apply(mapFileName: String) = new Mapper(mapFileName)
}