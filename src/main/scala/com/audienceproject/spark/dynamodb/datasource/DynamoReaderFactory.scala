/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2019 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.datasource

import com.amazonaws.services.dynamodbv2.document.Item
import com.audienceproject.shaded.google.common.util.concurrent.RateLimiter
import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import com.audienceproject.spark.dynamodb.util.ResponsiveRateLimiter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._

class DynamoReaderFactory(connector: DynamoConnector,
                          schema: StructType)
    extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        if (connector.isEmpty) new EmptyReader
        else new ScanPartitionReader(partition.asInstanceOf[ScanPartition])
    }

    private class EmptyReader extends PartitionReader[InternalRow] {
        override def next(): Boolean = false

        override def get(): InternalRow = throw new IllegalStateException("Unable to call get() on empty iterator")

        override def close(): Unit = {}
    }

    private class ScanPartitionReader(scanPartition: ScanPartition) extends PartitionReader[InternalRow] {

        import scanPartition._

        private val pageIterator = connector.scan(partitionIndex, requiredColumns, filters).pages().iterator().asScala
        private val rateLimiter = new ResponsiveRateLimiter(connector.readLimit)

        private var innerIterator: Iterator[InternalRow] = Iterator.empty

        private var currentRow: InternalRow = _
        private var proceed = false

        private val typeConversions = schema.collect({
            case StructField(name, dataType, _, _) => name -> TypeConversion(name, dataType)
        }).toMap

        override def next(): Boolean = {
            proceed = true
            innerIterator.hasNext || {
                if (pageIterator.hasNext) {
                    nextPage()
                    next()
                }
                else false
            }
        }

        override def get(): InternalRow = {
            if (proceed) {
                currentRow = innerIterator.next()
                proceed = false
            }
            currentRow
        }

        override def close(): Unit = {}

        private def nextPage(): Unit = {
            val page = pageIterator.next()
            val result = page.getLowLevelResult
            Option(result.getScanResult.getConsumedCapacity).foreach(cap => rateLimiter.acquire(cap.getCapacityUnits.toInt max 1))
            innerIterator = result.getItems.iterator().asScala.map(itemToRow(requiredColumns))
        }

        private def itemToRow(requiredColumns: Seq[String])(item: Item): InternalRow =
            if (requiredColumns.nonEmpty) InternalRow.fromSeq(requiredColumns.map(columnName => typeConversions(columnName)(item)))
            else InternalRow.fromSeq(item.asMap().asScala.values.toSeq.map(_.toString))

    }

}
