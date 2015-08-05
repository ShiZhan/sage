package settings

/**
 * @author ShiZhan
 * parsing config file
 */
object Config {
  import com.typesafe.config.ConfigFactory
  import helper.Resource

  val configString = Resource.getString("sage.conf")
  val config = ConfigFactory.parseString(configString)
  val nBuffersPerScanner = config.getInt("buffer.nBuffersPerScanner")
  val nEdgesPerBuffer = config.getInt("buffer.nEdgesPerBuffer")
}
