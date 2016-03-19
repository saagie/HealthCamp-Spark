import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Created by aurelien on 26/02/16.
  */
object HealthCamp extends App{

  case class CLIParams(
                        hdfsNameNodeHost: String = "",
                        hdfsPathCheckpoint: String = "",
                        hdfsDamirPath: String = "",
                        mongoHost: String = "",
                        mongoUser: String = "",
                        mongoPassword: String = ""
                      )

  val appName: String = getClass.getSimpleName

  val sqlContext = createContext(appName)

  val parser = parseArgs(appName)
  parser.parse(args, CLIParams()) match {
    case Some(config) =>
      System.setProperty("HADOOP_USER_NAME", "service")

      val damirPath = config.hdfsNameNodeHost + config.hdfsDamirPath

      // Read file from HDFS
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "DROPMALFORMED")
        .load(damirPath)

      df.registerTempTable("damir")

      val avgRefund = sqlContext.sql("select FLX_ANN_MOI as annee_mois," +
        " PSP_STJ_SNDS as statut_juridique_pro_sante, " +
        "avg(cast (PRS_REM_BSE as double)) as moy_montant_base_remboursement " +
        "from damir " +
        "group by PSP_STJ_SNDS, FLX_ANN_MOI")

      val mongoHost  = config.mongoHost

      val credentials = config.mongoUser + ",health_camp," + config.mongoPassword

      val options = Map("host" -> mongoHost, "database" -> "health_camp", "collection" -> "avgRefund", "credentials" -> credentials)
      avgRefund.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()

    case None =>
    // arguments are bad, error message will have been displayed

  }

  def createContext(appName: String): SQLContext = {

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.shuffle.partitions","20")
      .set("spark.executor.memory", "6G")
      .set("spark.speculation", "true")

    new org.apache.spark.sql.SQLContext(new SparkContext(sparkConf))

  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "version")
      help("help") text "prints this usage text"

      opt[String]("hdfsNameNodeHost") required() action { (data, conf) =>
        conf.copy(hdfsNameNodeHost = data)
      } text "URI of hdfs nameNode. Example : hdfs://IP:8020"

      opt[String]("hdfsDamirPath") required() action { (data, conf) =>
        conf.copy(hdfsDamirPath = data)
      } text "Directory path where the hdfsDamirPath data are : Example: /user/hdfs/hdfsDamirPath"

      opt[String]("mongoHost") required() action { (data, conf) =>
        conf.copy(mongoHost = data)
      } text "The host for MongoDB"

      opt[String]("mongoUser") required() action { (data, conf) =>
        conf.copy(mongoUser = data)
      } text "The user for MongoDB"

      opt[String]("mongoPassword") required() action { (data, conf) =>
        conf.copy(mongoPassword = data)
      } text "The password for MongoDB"
    }
  }



}
