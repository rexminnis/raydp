/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}

object Spark411SQLHelper {
  def toArrowSchema(schema: StructType, timeZoneId: String, largeVarTypes: Boolean = false): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = largeVarTypes)
  }

  def toArrowBatchRdd(df: DataFrame): org.apache.spark.rdd.RDD[Array[Byte]] = {
    val conf = df.sparkSession.asInstanceOf[ClassicSparkSession].sessionState.conf
    val timeZoneId = conf.sessionLocalTimeZone
    val maxRecordsPerBatch = conf.getConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val schema = df.schema
    df.queryExecution.toRdd.mapPartitions(iter => {
      val context = TaskContext.get()
      ArrowConverters.toBatchIterator(
        iter,
        schema,
        maxRecordsPerBatch,
        timeZoneId,
        true, // errorOnDuplicatedFieldNames
        largeVarTypes,
        context)
    })
  }

  def fromArrowBatchBytes(arrowIpcBytes: Array[Byte], schemaJson: String, timeZoneId: String): Iterator[org.apache.spark.sql.catalyst.InternalRow] = {
    import org.apache.arrow.vector.VectorUnloader
    import org.apache.arrow.vector.ipc.ArrowStreamReader
    import org.apache.arrow.vector.ipc.WriteChannel
    import org.apache.arrow.vector.ipc.message.MessageSerializer
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
    import java.nio.channels.Channels
    import scala.collection.mutable.ArrayBuffer

    if (arrowIpcBytes == null || arrowIpcBytes.isEmpty) {
      return Iterator.empty
    }

    val structType = DataType.fromJson(schemaJson).asInstanceOf[StructType]

    // Read the Python IPC stream and re-serialize each batch to Spark's
    // expected Arrow batch message format (MessageSerializer.serialize).
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      "raydp-ipc-reader", 0, Long.MaxValue)
    val batchBytesList = new ArrayBuffer[Array[Byte]]()

    try {
      val reader = new ArrowStreamReader(
        new ByteArrayInputStream(arrowIpcBytes), allocator)
      try {
        while (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          val unloader = new VectorUnloader(root)
          val arrowBatch = unloader.getRecordBatch()
          try {
            val out = new ByteArrayOutputStream()
            val channel = new WriteChannel(Channels.newChannel(out))
            MessageSerializer.serialize(channel, arrowBatch)
            channel.close()
            batchBytesList += out.toByteArray()
          } finally {
            arrowBatch.close()
          }
        }
      } finally {
        reader.close()
      }
    } finally {
      allocator.close()
    }

    // Feed the Spark-format batch bytes to ArrowConverters.fromBatchIterator
    ArrowConverters.fromBatchIterator(
      batchBytesList.iterator,
      structType,
      timeZoneId,
      true,  // errorOnDuplicatedFieldNames
      false, // largeVarTypes
      TaskContext.get()
    )
  }

  /**
   * Converts a JavaRDD of Arrow batches (serialized as byte arrays) to a DataFrame.
   * This is the reverse operation of toArrowBatchRdd.
   *
   * @param rdd     JavaRDD containing Arrow batches serialized as byte arrays
   * @param schema  JSON string representation of the StructType schema
   * @param session SparkSession to use for DataFrame creation
   * @return DataFrame reconstructed from the Arrow batches
   */
  def toDataFrame(rdd: JavaRDD[Array[Byte]], schema: String, session: SparkSession): DataFrame = {
    val structType = DataType.fromJson(schema).asInstanceOf[StructType]
    val classicSession = session.asInstanceOf[ClassicSparkSession]
    
    // Capture timezone and largeVarTypes on driver side - cannot access sessionState on executors
    val timeZoneId = classicSession.sessionState.conf.sessionLocalTimeZone
    val largeVarTypes = classicSession.sessionState.conf.arrowUseLargeVarTypes

    // Create an RDD of InternalRow by deserializing Arrow batches per partition
    val rowRdd = rdd.rdd.flatMap { arrowBatch =>
      ArrowConverters.fromBatchIterator(
        Iterator(arrowBatch),
        structType,
        timeZoneId,  // Use captured value, not sessionState
        true,  // errorOnDuplicatedFieldNames
        largeVarTypes,
        TaskContext.get()
      )
    }

    classicSession.internalCreateDataFrame(rowRdd, structType)
  }
}
