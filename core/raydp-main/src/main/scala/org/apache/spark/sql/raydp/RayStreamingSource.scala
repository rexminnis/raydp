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

package org.apache.spark.sql.raydp

import java.util

import com.intel.raydp.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table => CatalogTable, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

class RayStreamingOffset(val batchId: Int) extends Offset {
  override def json(): String = s"""{"batchId":$batchId}"""

  override def equals(other: Any): Boolean = other match {
    case o: RayStreamingOffset => batchId == o.batchId
    case _ => false
  }

  override def hashCode(): Int = batchId
}

object RayStreamingOffset {
  def fromJson(json: String): RayStreamingOffset = {
    val pattern = """"batchId"\s*:\s*(-?\d+)""".r
    pattern.findFirstMatchIn(json) match {
      case Some(m) => new RayStreamingOffset(m.group(1).toInt)
      case None => throw new IllegalArgumentException(s"Invalid offset JSON: $json")
    }
  }
}


// ---------------------------------------------------------------------------
// InputPartition — carries Arrow IPC bytes for one batch
// ---------------------------------------------------------------------------

class RayStreamingInputPartition(
    val arrowIpcBytes: Array[Byte],
    val schemaJson: String,
    val timeZoneId: String
) extends InputPartition with Serializable


// ---------------------------------------------------------------------------
// PartitionReader — converts Arrow IPC to InternalRow
// ---------------------------------------------------------------------------

class RayStreamingPartitionReader(partition: RayStreamingInputPartition)
    extends PartitionReader[InternalRow] {

  private val iter: Iterator[InternalRow] =
    SparkShimLoader.getSparkShims.fromArrowBatchBytes(
      partition.arrowIpcBytes, partition.schemaJson, partition.timeZoneId)

  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      current = iter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {}
}


// ---------------------------------------------------------------------------
// PartitionReaderFactory
// ---------------------------------------------------------------------------

class RayStreamingPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new RayStreamingPartitionReader(
      partition.asInstanceOf[RayStreamingInputPartition])
  }
}


// ---------------------------------------------------------------------------
// MicroBatchStream — reads from RayStreamingState
// ---------------------------------------------------------------------------

class RayStreamingMicroBatchStream(state: RayStreamingState, schema: StructType)
    extends MicroBatchStream {

  override def initialOffset(): Offset = new RayStreamingOffset(-1)

  override def latestOffset(): Offset = {
    new RayStreamingOffset(state.getLatestBatchId)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startId = start.asInstanceOf[RayStreamingOffset].batchId
    val endId = end.asInstanceOf[RayStreamingOffset].batchId

    (startId + 1 to endId).flatMap { batchId =>
      val data = state.getBatchData(batchId, 30000L) // 30s timeout
      if (data != null) {
        Some(new RayStreamingInputPartition(data, state.getSchemaJson, state.getTimeZoneId)
          .asInstanceOf[InputPartition])
      } else {
        None
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new RayStreamingPartitionReaderFactory()

  override def deserializeOffset(json: String): Offset =
    RayStreamingOffset.fromJson(json)

  override def commit(end: Offset): Unit = {
    val endId = end.asInstanceOf[RayStreamingOffset].batchId
    state.commit(endId + 1) // commit batches 0..endId inclusive
  }

  override def stop(): Unit = {
    RayStreamingState.remove(state.getStreamId)
  }
}


// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

class RayStreamingScan(state: RayStreamingState, schema: StructType)
    extends Scan {

  override def readSchema(): StructType = schema

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new RayStreamingMicroBatchStream(state, schema)
  }
}


// ---------------------------------------------------------------------------
// ScanBuilder
// ---------------------------------------------------------------------------

class RayStreamingScanBuilder(state: RayStreamingState, schema: StructType)
    extends ScanBuilder {

  override def build(): Scan = new RayStreamingScan(state, schema)
}


// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

class RayStreamingTable(state: RayStreamingState)
    extends CatalogTable with SupportsRead {

  private val _schema: StructType =
    DataType.fromJson(state.getSchemaJson).asInstanceOf[StructType]

  override def name(): String = s"RayStreaming[${state.getStreamId}]"

  override def schema(): StructType = _schema

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new RayStreamingScanBuilder(state, _schema)
  }
}


// ---------------------------------------------------------------------------
// TableProvider — entry point for spark.readStream.format(...)
// ---------------------------------------------------------------------------

class RayStreamingTableProvider extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val streamId = options.get("stream_id")
    require(streamId != null, "Option 'stream_id' is required")
    val state = RayStreamingState.get(streamId)
    require(state != null, s"No RayStreamingState registered for stream '$streamId'")
    DataType.fromJson(state.getSchemaJson).asInstanceOf[StructType]
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): CatalogTable = {
    val streamId = properties.get("stream_id")
    require(streamId != null, "Option 'stream_id' is required")
    val state = RayStreamingState.get(streamId)
    require(state != null, s"No RayStreamingState registered for stream '$streamId'")
    new RayStreamingTable(state)
  }
}
