package graph

abstract class Vertices[ValueType] {
  import java.util.concurrent.ConcurrentNavigableMap
  type VertexTable = ConcurrentNavigableMap[Long, ValueType]
  def getVertexTable(name: String): VertexTable
}

class VerticesMEMDB[ValueType] extends Vertices[ValueType] {
  import org.mapdb.DBMaker

  private val db = DBMaker.newMemoryDB().closeOnJvmShutdown().make()
  def commit() = db.commit()
  def close() = db.close()
  def getVertexTable(name: String): VertexTable = db.getTreeMap(name)
}

class VerticesTempDB[ValueType] extends Vertices[ValueType] {
  import org.mapdb.DBMaker

  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  def commit() = db.commit()
  def close() = db.close()
  def getVertexTable(name: String): VertexTable = db.getTreeMap(name)
}

class VerticesFileDB[ValueType](verticesFile: String) extends Vertices[ValueType] {
  import java.io.File
  import org.mapdb.DBMaker

  private val f = new File(verticesFile)
  private val db = DBMaker.newFileDB(f).closeOnJvmShutdown().make()
  def commit() = db.commit()
  def close() = db.close()
  def getVertexTable(name: String): VertexTable = db.getTreeMap(name)
}