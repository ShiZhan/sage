package settings

/**
 * @author ShiZhan
 * parsing config file
 */
object Config {
  import com.typesafe.config.ConfigFactory

  val config = ConfigFactory.parseResources("sage.conf")
  val nBuffersPerScanner = config.getInt("buffer.nBuffersPerScanner")
  val nEdgesPerBuffer = config.getInt("buffer.nEdgesPerBuffer")
}
