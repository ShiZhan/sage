package sage.test

object ParallelIterationTest {
  import java.util.concurrent.ThreadLocalRandom
  import java.security.MessageDigest
  import helper.Timing._

  def someOps = {
    val md = MessageDigest.getInstance("SHA1")
    val bytes = Array.fill(1 << 25)(0.toByte)
    ThreadLocalRandom.current().nextBytes(bytes)
    md.update(bytes)
    md.digest().map("%02x".format(_)).mkString
  }

  val r = 1 to 1 << 3

  def generic = () => for (i <- r) yield someOps

  def parallel = () => for (i <- r.par) yield someOps

  def main(args: Array[String]) = {
    val (result0, e0) = { generic }.elapsed
    println(s"$result0, $e0")
    val (result1, e1) = { parallel }.elapsed
    println(s"$result1, $e1")
  }
}
