package Utils



import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import com.mongodb.casbah.{MongoClient, MongoCollection}
import com.mongodb.{MongoCredential, ServerAddress}
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

object DBConnectionMongoDB {

  def connectMongoDB (): MongoCollection = {
    val hostname = "localhost"
    val port = 11224
    val userName = "jon"
    val database = "jonsnow"
    val password = "snow"
    val collection = "prisms"

    val server = new ServerAddress(hostname, port)
    val credentials = MongoCredential.createCredential(userName, database, password.toCharArray)
    val mongoClient= MongoClient(server, List(credentials))
    println("Connect MongoDB Successfully!!!")
    val db = mongoClient.getDB(database)
    db(collection)
  }

  def dbWriteData(df: DataFrame,
                  time: Timestamp):
  Unit = {

    val collection = this.connectMongoDB()
    val id = df.schema.fields.head.name
    val value = df.schema.fields(2).name

    val values = df.rdd.map(x => (x.getAs[String](id).toInt, x.getAs[Double](value)))
      .map(x => MongoDBObject("gid" -> x._1, "aqi" -> x._2)).collect()

    val mongoDBList = new MongoDBList()
    for (each <- values) mongoDBList += each

    val insertAll = MongoDBObject("timestamp"-> new DateTime(time.getTime).toDate, "data" -> mongoDBList)
    collection.insert(insertAll)
    println(s"Insert $time Successfully!!!")
  }
}
