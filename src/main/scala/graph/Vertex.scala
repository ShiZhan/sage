package graph

class Vertices(verticesFN: String) {
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  type VertexTable = ConcurrentNavigableMap[Long, Long]

  private var stepCounter = 0
  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private def step(i: Int) = db.getTreeMap(s"$i").asInstanceOf[VertexTable]

  def in = step(stepCounter)
  def out = step(stepCounter + 1)

  def nextStep = {
    in.clear()
    stepCounter += 1
  }

  def commit() = db.commit()
  def close() = db.close()
}

object Vertices {
  def apply(verticesFN: String) = new Vertices(verticesFN)
}