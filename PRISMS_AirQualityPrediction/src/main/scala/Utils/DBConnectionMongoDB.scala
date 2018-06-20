//package Utils
//
//import java.util
//import java.util.Properties
//
//import com.mongodb.{MongoCredential, ServerAddress}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.mongodb.scala.MongoClient
//
//object DBConnectionMongoDB {
//
//  def dbJDBC: String = {
//    val hostname = "localhost"
//    val port = 11223
//    val collection = "jonsnow"
//    val url = s"mongodb://$hostname:$port/$database"
//    url
//  }
//
//  def connProperties: Properties = {
//    val properties = new Properties()
//    properties.put("user", "jon")
//    properties.put("password", "snow")
//    properties.put("Driver", "org.postgresql.Driver")
//    properties
//  }
//
//  def dbReadData(tableName: String,
//                 cols: List[String],
//                 conditions: String,
//                 sparkSession: SparkSession):
//  DataFrame = {
//
//    val colString = cols.mkString(",")
//    val query = s"(select $colString from $tableName $conditions) as sub"
//    val data = sparkSession.read.jdbc(
//      url = this.dbJDBC,
//      table = query,
//      properties = this.connProperties
//    )
//    data
//  }
//
//  def dbWriteData(df: DataFrame,
//                  schema: String,
//                  tableName: String):
//  Unit = {
//
//    val hostname = "localhost"
//    val port = 11223
//    val userName = "jon"
//    val database = "jonsnow"
//    val password = "snow"
//
//    try {
//
//      var serverAddress = new ServerAddress(hostname, port)
//      val adds = new util.ArrayList[ServerAddress]()
//      adds.add(serverAddress)
//
//      var credential = MongoCredential.createCredential(userName, database, password.toCharArray)
//      val credentials = new util.ArrayList[MongoCredential]()
//      credentials.add(credential)
//
//      val mongoClient = new MongoClient(adds, credentials)
//
//      val mongoDatabase = mongoClient.getDatabase(database)
////      val collection:MongoCollection[Document] = mongoDatabase.getCollection("price_discount")
//
//
//    }
//
//
//
//    catch {
//      case e:Exception=>
//        println(e.getClass.getName + ": " + e.getMessage)
//    }
//  }
//
//}
