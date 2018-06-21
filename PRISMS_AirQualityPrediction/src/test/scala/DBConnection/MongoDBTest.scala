package DBConnection

import java.text.SimpleDateFormat

import com.mongodb.{MongoCredential, ServerAddress}
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import java.util.ArrayList

import com.mongodb.casbah.MongoClient
import org.joda.time.DateTime

object MongoDBTest {


  def main(args: Array[String]): Unit = {

    val hostname = "localhost"
    val port = 11224
    val userName = "jon"
    val database = "jonsnow"
    val password = "snow"

    val server = new ServerAddress(hostname, 11224)
    val credentials = MongoCredential.createCredential(userName, database, password.toCharArray)
    val mongoClient= MongoClient(server, List(credentials))

    val db = mongoClient.getDB(database)
    println("Connect MongoDB Successfully!!!")

    val coll = db("prisms")

    val t = coll.find()



    println(t)

    val test = MongoDBObject("timestamp"-> new DateTime(1529528400000L).toDate,
      "data" -> MongoDBList(MongoDBObject("gid" -> 3, "aqi" -> 2.6), MongoDBObject("gid" -> 4, "aqi" -> 23.6)))

    coll.insert(test)
    println(coll.count())

  }
}

//    val serverAddress = new ServerAddress(hostname,port)
//    val adds = new ArrayList[ServerAddress]()
//    adds.add(serverAddress)
//
//    val credential = MongoCredential.createCredential(userName, database, password.toCharArray)
//    val credentials = new ArrayList[MongoCredential]()
//    credentials.add(credential)
//
//    val mongoClient = new MongoClient(adds, credentials)
//    val mongoDatabase = mongoClient.getDatabase(database)
//
//    val coll = mongoDatabase.getCollection("prisms")
//    println(coll.count())
