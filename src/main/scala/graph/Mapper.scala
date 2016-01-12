package graph

/**
 * @author Zhan
 * edge list mapper
 * mapFileName: map ID in "edgeFileName" by "mapFileName" to mapped file
 */
object Mapper {
  import java.io.{ File, PrintWriter }
  import scala.collection.mutable.Map
  import scala.io.Source
  import helper.IteratorOps.VisualOperations

  val id = Iterator.iterate(0L)(_ + 1)
  val vMap = Map[Long, Long]()
  val mapEdge: PartialFunction[SimpleEdge, SimpleEdge] = {
    case Edge(u, v) =>
      Edge(vMap.getOrElseUpdate(u, id.next), vMap.getOrElseUpdate(v, id.next))
  }

  def run(mapFileName: String, edgeFileName: String, binary: Boolean) = {
    val usePipe = edgeFileName.isEmpty
    val mapFile = new File(mapFileName)
    if (mapFile.exists) {
      if (!usePipe) println(s"Loading vertex map from $mapFileName ...")
      for (v <- Source.fromFile(mapFile).getLines()) vMap.put(v.toInt, id.next)
    }

    if (!usePipe) println(s"Permutating $edgeFileName ...")
    val edgeProvider = if (usePipe) Edges.fromConsole
    else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges = edgeProvider.getEdges
    val mappedEdges = edges.map(mapEdge)
    val edgeStorage = if (usePipe) Edges.fromConsole
    else if (binary) Edges.fromFile(s"$edgeFileName-mapped.bin")
    else Edges.fromText(s"$edgeFileName-mapped.edges")
    edgeStorage.putEdges(mappedEdges)

    if (!usePipe) println(s"Updating vertex map file $mapFileName ...")
    val pw = new PrintWriter(mapFile)
    vMap.toArray.sortBy(_._2).map(_._1).iterator.map(_.toString).foreachDo(pw.println)
    pw.close()

    if (!usePipe) println(s"Done.")
  }
}