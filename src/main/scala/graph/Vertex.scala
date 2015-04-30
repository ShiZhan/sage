package graph

case class Vertex(value: Long, renew: Boolean) {
  override def toString = s"value: $value, renew: $renew"
}

class Vertices(verticesFN: String) {
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private val vertices = db.getTreeMap("vertices").asInstanceOf[ConcurrentNavigableMap[Long, Vertex]]

  def get(id: Long) = vertices.get(id)
  def put(id: Long, vertex: Vertex) = vertices.put(id, vertex)

  def commit() = db.commit()
  def close() = db.close()
}

object Vertices {
  def apply(verticesFN: String) = new Vertices(verticesFN)
}