package graph

class Vertices(verticesFN: String) extends helper.Logging {
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  type VertexTable = ConcurrentNavigableMap[Long, Long]

  private var stepCounter = 0
  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private def step(i: Int) = db.getTreeMap(s"$i").asInstanceOf[VertexTable]

  val data = db.getTreeMap("status").asInstanceOf[VertexTable]
  def in = step(stepCounter)
  def out = step(stepCounter + 1)

  def update = {
    val gathered = in.size()
    val scattered = out.size()
    data.putAll(out)
    in.clear()
    stepCounter += 1
    logger.info("step [{}] (gather, scatter): [{}]", stepCounter, (gathered, scattered))
  }

  def commit() = db.commit()
  def close() = db.close()
}

object Vertices {
  def apply(verticesFN: String) = new Vertices(verticesFN)
}