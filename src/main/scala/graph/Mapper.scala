package graph

/**
 * @author Zhan
 * edge list mapper
 * mapFileName: map ID in "edgeFileName" by "mapFileName" to mapped file
 * provide empty or non-exist mapfile will "squeeze" input edge list
 * new IDs will be assigned consecutively
 */
object Mapper {
  import java.io.{ File, PrintWriter }
  import scala.io.Source
  import helper.GrowingArray
  import helper.IteratorOps.VisualOperations

  val id = Iterator.iterate(0L)(_ + 1)
  val vMap = GrowingArray[Long](-1)
  val mapEdge: PartialFunction[SimpleEdge, SimpleEdge] = {
    case Edge(u, v) =>
      if (vMap.unVisited(u)) vMap(u) = id.next
      if (vMap.unVisited(v)) vMap(v) = id.next
      Edge(vMap(u), vMap(v))
  }

  def run(mapFileName: String, edgeFileName: String, binary: Boolean) = {
    val usePipe = edgeFileName.isEmpty
    val mapFile = new File(mapFileName)
    if (mapFile.exists) {
      System.err.println(s"Loading vertex map from $mapFileName ...")
      for (
        line <- Source.fromFile(mapFile).getLines();
        elems = line.split(" ");
        (before, after) = (elems(0).toLong, elems(1).toLong) if elems.length == 2
      ) {
        vMap(before) = after
        id.next // mapped IDs may not be consecutively allocated, may change to max(after) in the future.
      }
    }

    System.err.println(s"Permutating $edgeFileName ...")
    val edgeProvider = if (usePipe) Edges.fromConsole
    else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges = edgeProvider.getEdges
    val mappedEdges = edges.map(mapEdge)
    val edgeStorage = if (usePipe) Edges.fromConsole
    else if (binary) Edges.fromFile(s"$edgeFileName-mapped.bin")
    else Edges.fromText(s"$edgeFileName-mapped.edges")
    edgeStorage.putEdges(mappedEdges)

    System.err.println(s"Updating vertex map file $mapFileName ...")
    val pw = new PrintWriter(mapFile)
    vMap.updated.map { case (before, after) => s"$before $after" }.foreachDo(pw.println)
    pw.close()

    System.err.println(s"Done.")
  }
}