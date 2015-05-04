package graph

class Vertices(verticesFN: String) extends helper.Logging {
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  type VertexTable = ConcurrentNavigableMap[Long, Long]

  private var stepCounter = 0
  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private def step(i: Int) = db.getTreeMap(s"$i").asInstanceOf[VertexTable]

  def in = step(stepCounter)
  def out = step(stepCounter + 1)

  def nextStep = {
    val gathered = in.size()
    val scattered = out.size()
    in.clear()
    stepCounter += 1
    logger.info("(gather, scatter): [{}], go to step [{}]", (gathered, scattered), stepCounter)
  }

  def commit() = db.commit()
  def close() = db.close()
}

object Vertices {
  def apply(verticesFN: String) = new Vertices(verticesFN)
}