/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.validator;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Validator;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class ValidatorRecorBatch extends AbstractRecordBatch<Validator> {

  private final RecordBatch target;
  private final RecordBatch source;
  private final List<String> columns;


  public ValidatorRecorBatch(Validator config, RecordBatch target, RecordBatch source, FragmentContext context) {
    super(config, context, false);
    this.target = target;
    this.source = source;
    this.columns = config.getColumns();
  }

  @Override
  public IterOutcome innerNext() {
    // compare target row column names and column (should match, otherwise throw exception)
    return null; //todo
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    target.kill(sendUpstream);
    source.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return source.getRecordCount(); //todo do we need to add target
  }
}

