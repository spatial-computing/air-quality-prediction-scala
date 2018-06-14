package dataDef

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.Constants

import scala.collection.mutable.ArrayBuffer

class GeoFeature (landusagesAreaRDD: RDD[OpenStreetMap],
                  waterareasAreaRDD: RDD[OpenStreetMap],
                  roadsLengthRDD: RDD[OpenStreetMap],
                  aerowaysLengthRDD: RDD[OpenStreetMap],
                  buildingsNumRDD: RDD[OpenStreetMap],
                  oceanDistanceRDD: RDD[OpenStreetMap]) extends java.io.Serializable {

  var landusageAreas = landusagesAreaRDD
  var waterAreas = waterareasAreaRDD
  var roadsLength = roadsLengthRDD
  var aerowaysLength = aerowaysLengthRDD
  var buildingsNum = buildingsNumRDD
  var oceanDistance = oceanDistanceRDD

  def getFeatureName(): (ArrayBuffer[(String, Int, String)]) = {

    val landusageType = landusageAreas.map(x => x.featureType).distinct().collect().sorted
    val waterareaType = waterAreas.map(x => x.featureType).distinct().collect().sorted
    val roadType = roadsLength.map(x => x.featureType).distinct().collect().sorted
    val aerowaysType = aerowaysLength.map(x => x.featureType).distinct().collect().sorted
    val buildingType = buildingsNum.map(x => x.featureType).distinct().collect().sorted

    val featureName = new ArrayBuffer[(String, Int, String)]()

    for (eachBufferSize <- Constants.bufferSize) {
      for (eachType <- landusageType) {
        val featureN = ("landusages", eachBufferSize, eachType)
        featureName += featureN
      }
      for (eachType <- waterareaType) {
        val featureN = ("waterareas", eachBufferSize, eachType)
        featureName += featureN
      }
      for (eachType <- roadType) {
        val featureN = ("roads", eachBufferSize, eachType)
        featureName += featureN
      }
      for (eachType <- aerowaysType) {
        val featureN = ("aeroways", eachBufferSize, eachType)
        featureName += featureN
      }
      for (eachType <- buildingType) {
        val featureN = ("buildings", eachBufferSize, eachType)
        featureName += featureN
      }
    }
    val featureN = ("ocean", 0, "distance")
    featureName += featureN
    return featureName
  }

  def geoFeatureVectorConstruction(reportingAreas: Array[String],
                                   geoFeature: ArrayBuffer[(String, Int, String)]):
  (ArrayBuffer[(String, Array[Double])]) = {

    val landusageMap = landusageAreas.map(x => ((x.reportingArea, x.featureType, x.bufferSize), x.value)).collectAsMap()
    val waterareaMap = waterAreas.map(x => ((x.reportingArea, x.featureType, x.bufferSize), x.value)).collectAsMap()
    val roadMap = roadsLength.map(x => ((x.reportingArea, x.featureType, x.bufferSize), x.value)).collectAsMap()
    val aerowaysMap = aerowaysLength.map(x => ((x.reportingArea, x.featureType, x.bufferSize), x.value)).collectAsMap()
    val buildingMap = buildingsNum.map(x => ((x.reportingArea, x.featureType, x.bufferSize), x.value)).collectAsMap()
    val oceanMap = oceanDistance.map(x => (x.reportingArea, x.value)).collectAsMap()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Construct the geo vector accordingly
    var featureVector : ArrayBuffer[(String, Array[Double])] = new ArrayBuffer[(String, Array[Double])]
    for (eachArea <- reportingAreas) {
      var tmpArrayBuffer: ArrayBuffer[Double] = new ArrayBuffer[Double]()

      for (eachGeoFeature <- geoFeature) {
        val eachBufferSize = eachGeoFeature._2
        val eachType = eachGeoFeature._3
        if (eachGeoFeature._1 == "landusages" && landusageMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += landusageMap {
            (eachArea, eachType, eachBufferSize)
          }
        else if (eachGeoFeature._1 == "waterareas" && waterareaMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += waterareaMap {
            (eachArea, eachType, eachBufferSize)
          }
        else if (eachGeoFeature._1 == "roads" && roadMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += roadMap {
            (eachArea, eachType, eachBufferSize)
          }
        else if (eachGeoFeature._1 == "aeroways" && aerowaysMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += aerowaysMap {
            (eachArea, eachType, eachBufferSize)
          }
        else if (eachGeoFeature._1 == "buildings" && buildingMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += buildingMap {
            (eachArea, eachType, eachBufferSize)
          }
        else if (eachGeoFeature._1 == "ocean" && aerowaysMap.contains((eachArea, eachType, eachBufferSize)))
          tmpArrayBuffer += oceanMap(eachArea)
        else
          tmpArrayBuffer += 0.0
      }

      featureVector += ((eachArea, tmpArrayBuffer.toArray))
    }

    return (featureVector)
  }

}
