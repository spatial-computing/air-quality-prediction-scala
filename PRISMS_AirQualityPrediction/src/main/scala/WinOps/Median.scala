package WinOps

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

object Median extends UserDefinedAggregateFunction {

  // Data types of input arguments of this aggregate function
  override def inputSchema: StructType = StructType(StructField("input", DoubleType) :: Nil)

  // Data types of values in the aggregation buffer
  override def bufferSchema: StructType = StructType(StructField("buffer", ArrayType(DoubleType, containsNull = false)) :: Nil)

  // DataType of the returned value of this UDAF
  override def dataType: DataType = DoubleType

  // Whether this function always returns the same output on the identical input
  override def deterministic: Boolean = true

  // Initializes the given aggregation buffer.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.WrappedArray.empty[DoubleType]
    //buffer(0) = new scala.collection.mutable.ArrayBuffer[Double]()
  }

  // Updates the given aggregation buffer `buffer` with new input data from `input`
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getAs[mutable.WrappedArray[Double]](0).toBuffer += input.getDouble(0)
    }
  }

  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // buffer1(0) = buffer1.getAs[mutable.WrappedArray[Double]](0) ++: buffer2.getAs[mutable.WrappedArray[Double]](0)
    // We need fast concatenation and avoid copying the entire collection, thus usage of Buffers
    buffer1(0) = buffer1.getAs[mutable.WrappedArray[Double]](0).toBuffer ++= buffer2.getAs[mutable.WrappedArray[Double]](0).toBuffer
  }

  // Calculate the median from the given array
  override def evaluate(buffer: Row): Any = {

    val elems = buffer.getAs[mutable.WrappedArray[Double]](0).toArray.sorted
    val buffSize = elems.length

    val middle_index = buffSize / 2
    buffSize match {
      case 0 => None
      case even if buffSize % 2 == 0 => (elems(middle_index - 1) + elems(middle_index)) / 2
      case odd => elems(middle_index)
    }
  }

}
