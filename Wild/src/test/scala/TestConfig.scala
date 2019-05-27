import com.typesafe.config.{Config, ConfigFactory}
import com.hamshif.wielder.wild.{DatalakeArgParser, DatalakeConfig}
import org.apache.spark.internal.Logging


object TestConfig extends DatalakeArgParser with Logging {

  val EXAMPLE_CONF = "example_conf"

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)

    val typeSafeConf = getTypeSafeConfig(conf)

    println(s"typeSafeConf:\n\n$typeSafeConf")
  }


  def getTypeSafeConfig(conf: DatalakeConfig): Config = {

    val env = conf.env

    println(s"env: $env")

    val baseConfig = ConfigFactory.parseResources(s"$EXAMPLE_CONF/application.conf")
    val namespacesConfig = ConfigFactory.parseResources(s"$EXAMPLE_CONF/env/$env/namespaces.conf")
    val generalConfig = ConfigFactory.parseResources(s"$EXAMPLE_CONF/general.conf")

    val typeSafeConfig = generalConfig
      .withFallback(namespacesConfig)
      .withFallback(baseConfig)
      .resolve()

    logInfo(typeSafeConfig.toString)

    typeSafeConfig
  }

}
