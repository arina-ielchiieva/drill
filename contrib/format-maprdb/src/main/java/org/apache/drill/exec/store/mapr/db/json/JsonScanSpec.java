/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.mapr.db.json;

import java.nio.ByteBuffer;
import java.util.List;

import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.ojai.store.QueryCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mapr.db.impl.ConditionImpl;
import com.mapr.db.impl.ConditionNode.RowkeyRange;
import com.mapr.db.impl.MapRDBImpl;
import com.mapr.db.index.IndexDesc;

public class JsonScanSpec {
  protected String tableName;
  protected IndexDesc indexDesc;
  protected QueryCondition condition;

  protected byte[] startRow;
  protected byte[] stopRow;
  protected byte[] serializedFilter;

  public JsonScanSpec(String tableName, IndexDesc indexDesc, QueryCondition condition) {
    this.tableName = tableName;
    this.indexDesc = indexDesc;
    this.condition = condition;
    if (this.condition != null) {
      List<RowkeyRange> rkRanges = ((ConditionImpl)this.condition).getRowkeyRanges();
      if (rkRanges.size() > 0) {
        startRow = rkRanges.get(0).getStartRow();
        stopRow  = rkRanges.get(rkRanges.size() - 1).getStopRow();
      } else {
        startRow = HConstants.EMPTY_START_ROW;
        stopRow  = HConstants.EMPTY_END_ROW;
      }
    }
  }

  @JsonCreator
  public JsonScanSpec(@JsonProperty("tableName") String tableName,
                      @JsonProperty("indexDesc") IndexDesc indexDesc,
                      @JsonProperty("serializedFilter") byte[] serializedFilter,
                      @JsonProperty("startRow") byte[] startRow,
                      @JsonProperty("stopRow") byte[] stopRow) {
    this.tableName = tableName;
    this.indexDesc = indexDesc;
    this.serializedFilter = serializedFilter;
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  public String getTableName() {
    return this.tableName;
  }

  public IndexDesc getIndexDesc() {
    return this.indexDesc;
  }

  public void setStartRow(byte []startRow) {
    this.startRow = startRow;
  }

  public void setStopRow(byte []stopRow) {
    this.stopRow = stopRow;
  }

  public byte[] getStartRow() {
    return this.startRow;
  }

  public byte[] getStopRow() {
    return this.stopRow;
  }

  public byte[] getSerializedFilter() {
    if (this.condition != null) {
      ByteBuffer bbuf = ((ConditionImpl)this.condition).getDescriptor().getSerialized();
      byte[] serFilter = new byte[bbuf.limit() - bbuf.position()];
      bbuf.get(serFilter);
      return serFilter;
    }
    return serializedFilter;
  }

  public void setCondition(QueryCondition condition) {
    this.condition = condition;
  }

  @JsonIgnore
  public QueryCondition getCondition() {
    return this.condition;
  }

  @JsonIgnore
  public boolean isSecondaryIndex() {
    return (this.indexDesc != null);
  }

  @JsonIgnore
  public Path getPrimaryTablePath() {
    return (this.indexDesc == null) ? null : new Path(this.indexDesc.getPrimaryTablePath());
  }

  @JsonIgnore
  public String getIndexName() {
    return (this.indexDesc == null) ? null : this.indexDesc.getIndexName();
  }

  public void mergeScanSpec(String functionName, JsonScanSpec scanSpec) {
    if (this.condition != null && scanSpec.getCondition() != null) {
      QueryCondition newCond = MapRDBImpl.newCondition();
      switch (functionName) {
        case "booleanAnd":
          newCond.and();
          break;
        case "booleanOr":
          newCond.or();
          break;
        default:
            assert(false);
      }

      newCond.condition(this.condition)
             .condition(scanSpec.getCondition())
             .close()
             .build();

      this.condition = newCond;
    } else if (scanSpec.getCondition() != null){
      this.condition = scanSpec.getCondition();
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("JsonScanSpec [tableName=").append(tableName);
    if (condition != null) {
      builder.append(", condition=").append(condition.toString());
    } else {
      builder.append(", serializedFilter=").append(serializedFilter == null ? "" : Bytes.toBase64(serializedFilter));
      builder.append(", startRow=").append(startRow == null ? "" : Bytes.toStringBinary(startRow));
      builder.append(", stopRow=").append(stopRow == null ? "" : Bytes.toStringBinary(stopRow));
    }

    builder.append(", indexName=").append(indexDesc == null ? "" : getIndexName());
    return builder.toString();
  }
}
