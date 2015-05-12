package graph

class Vertices[T](verticesFile: String) extends helper.Logging {
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import scala.collection.JavaConversions._
  import org.mapdb.DBMaker

  type VertexTable = ConcurrentNavigableMap[Long, T]

  private val db =
    if (verticesFile.isEmpty)
      DBMaker.newTempFileDB().closeOnJvmShutdown().make()
    else
      DBMaker.newFileDB(new File(verticesFile)).closeOnJvmShutdown().make()
  def commit() = db.commit()
  def close() = db.close()

  private var stepCounter = 0
  private def step(i: Int) = db.getTreeMap(s"$i").asInstanceOf[VertexTable]
  val data = step(0)
  def in = step(stepCounter)
  def out = step(stepCounter + 1)

  def update = {
    val gathered = in.size()
    val scattered = out.size()
    in.clear()
    data.putAll(out)
    stepCounter += 1
    logger.info("step [{}] (gather, scatter): [{}]", stepCounter, (gathered, scattered))
  }

  def print =
    data.toIterator.foreach { case (k: Long, v: Any) => println(s"$k $v") }
}

object Vertices {
  def apply[T]() = new Vertices[T]("")
  def apply[T](verticesFile: String) = new Vertices[T](verticesFile)
}