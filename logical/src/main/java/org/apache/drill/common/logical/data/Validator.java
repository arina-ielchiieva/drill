/*
 * ****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */
package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("validator")
public class Validator extends LogicalOperatorBase {

  private final LogicalOperator target;
  private final LogicalOperator source;
  private final List<String> columns;

  @JsonCreator
  public Validator(@JsonProperty("target") LogicalOperator target,
                   @JsonProperty("source") LogicalOperator source,
                   @JsonProperty("columns") List<String> columns) {
    this.target = target;
    target.registerAsSubscriber(this);
    this.source = source;
    source.registerAsSubscriber(this);
    this.columns = columns;
  }

  public LogicalOperator getTarget() {
    return target;
  }

  public LogicalOperator getSource() {
    return source;
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitValidator(this, value);
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return Iterators.forArray(target, source);
  }

  public static Builder builder(){
    return new Builder();
  }

  public static class Builder extends AbstractBuilder<Validator> {
    private LogicalOperator target;
    private LogicalOperator source;
    private List<String> columns;

    public Builder target(LogicalOperator target) {
      this.target = target;
      return this;
    }

    public Builder source(LogicalOperator source) {
      this.source = source;
      return this;
    }

    public Builder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    @Override
    public Validator build() {
      Preconditions.checkNotNull(target);
      Preconditions.checkNotNull(source);
      Preconditions.checkNotNull(columns);
      return new Validator(target, source, columns);
    }
  }
}

