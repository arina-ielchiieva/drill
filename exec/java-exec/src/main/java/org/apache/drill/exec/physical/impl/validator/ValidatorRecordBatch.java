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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Validator;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ValidatorRecordBatch extends AbstractRecordBatch<Validator> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValidatorRecordBatch.class);

  private final ValidatorInput validatorInput;

  private List<MaterializedField> outputFields;
  private final List<TransferPair> transfers = Lists.newArrayList();
  private List<ValueVector> allocationVectors;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private int recordCount = 0;
  private boolean schemaAvailable = false; //todo ??


  public ValidatorRecordBatch(Validator config, RecordBatch left, RecordBatch right, FragmentContext context) {
    super(config, context, false);
    this.validatorInput = new ValidatorInput(this, left, right, config.getColumns());
  }

  @Override
  public IterOutcome innerNext() {
    // compare target row column names and column (should match, otherwise throw exception)
    try {
      IterOutcome upstream = validatorInput.nextBatch();
      logger.debug("Upstream of Validator: {}", upstream);
      switch (upstream) {
        case NONE:
        case OUT_OF_MEMORY:
        case STOP:
          return upstream;

        case OK_NEW_SCHEMA:
          outputFields = validatorInput.getOutputFields();
        case OK:
          IterOutcome workOutcome = doWork();

          if (workOutcome != IterOutcome.OK) {
            return workOutcome;
          } else {
            return upstream;
          }
        default:
          throw new IllegalStateException(String.format("Unknown state %s.", upstream));
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException ex) {
      context.fail(ex);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  @Override
  public int getRecordCount() {
    return validatorInput.getRightRecordBatch().getRecordCount(); //todo do we need to add target
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    validatorInput.getLeftRecordBatch().kill(sendUpstream);
    validatorInput.getRightRecordBatch().kill(sendUpstream);
  }

  private IterOutcome doWork() throws SchemaChangeException, IOException, ClassTransformationException {
    if (allocationVectors != null) {
      for (ValueVector v : allocationVectors) {
        v.clear();
      }
    }

    // If right side is empty
    if (validatorInput.rightIsFinish) {
      for (MaterializedField outputField : outputFields) {
        ValueVector vv = container.addOrGet(outputField, callBack);
        allocationVectors.add(vv);
      }

      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      return IterOutcome.OK; //todo OK_NEW_SCHEMA or OK?
    }

    allocationVectors = Lists.newArrayList();
    transfers.clear();

    final ClassGenerator<Converter> cg = CodeGenerator.getRoot(Converter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    // cg.getCodeGenerator().saveCodeForDebugging(true);

    int index = 0;
    for (VectorWrapper<?> vw : validatorInput.getRightRecordBatch()) {
      ValueVector vvIn = vw.getValueVector();
      // get the original input column names
      SchemaPath inputPath = SchemaPath.getSimplePath(vvIn.getField().getPath());
      // get the renamed column names
      SchemaPath outputPath = SchemaPath.getSimplePath(outputFields.get(index).getPath());

      final ErrorCollector collector = new ErrorCollectorImpl();
      // According to input data names, Minortypes, Datamodes, choose to
      // transfer directly,
      // rename columns or
      // cast data types (Minortype or DataMode)
      if (ValidatorInput.hasSameTypeAndMode(outputFields.get(index), vw.getValueVector().getField())) {
        // Transfer column
        if (outputPath.equals(inputPath)) {
          final LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath,
              validatorInput.getRightRecordBatch(), collector, context.getFunctionRegistry());
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema. " +
                "Errors:\n %s.", collector.toErrorString()));
          }
          ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          //todo we need to re-create materialize field? is this because of unescaped path?
          ValueVector vvOut = container.addOrGet(MaterializedField.create(outputPath.getAsUnescapedPath(), vectorRead.getMajorType()));
          TransferPair tp = vvIn.makeTransferPair(vvOut);
          transfers.add(tp);
        // Copy data in order to rename the column
        } else {
          final LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath,
              validatorInput.getRightRecordBatch(), collector, context.getFunctionRegistry());
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema. " +
                "Errors:\n %s.", collector.toErrorString()));
          }

          MaterializedField outputField = MaterializedField.create(outputPath.getAsUnescapedPath(), expr.getMajorType());
          ValueVector vv = container.addOrGet(outputField, callBack);
          allocationVectors.add(vv);
          TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getPath()));
          ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
          cg.addExpr(write);
        }
      // Cast is necessary
      } else {
        //todo repeats all the time
        LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath,
            validatorInput.getRightRecordBatch(), collector, context.getFunctionRegistry());
        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema. " +
              "Errors:\n %s.", collector.toErrorString()));
        }

        // If inputs DataMode is required but outputs data type is optional
        // convert input to less restrictive type
        //todo investigate REPEATED data mode
        if (vvIn.getField().getType().getMode() == TypeProtos.DataMode.OPTIONAL &&
            outputFields.get(index).getType().getMode() == TypeProtos.DataMode.REQUIRED) {
          expr = ExpressionTreeMaterializer.convertToNullableType(expr, outputFields.get(index).getType().getMinorType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        // If input minor type is different, insert cast operation
        if (vvIn.getField().getType().getMinorType() != outputFields.get(index).getType().getMinorType()) {
          expr = ExpressionTreeMaterializer.addCastExpression(expr, outputFields.get(index).getType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        final MaterializedField outputField = MaterializedField.create(outputPath.getAsUnescapedPath(), expr.getMajorType());
        ValueVector vector = container.addOrGet(outputField, callBack);
        allocationVectors.add(vector);
        TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getPath()));

        boolean useSetSafe = !(vector instanceof FixedWidthVector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        cg.addExpr(write);
      }
      ++index;
    }

    Converter converter = context.getImplementationClass(cg.getCodeGenerator());
    converter.setup(context, validatorInput.getRightRecordBatch(), this, transfers);

    if (!schemaAvailable) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      schemaAvailable = true;
    }

    if(!doAlloc()) {
      return IterOutcome.OUT_OF_MEMORY;
    }

    recordCount = converter.convertRecords(0, validatorInput.getRightRecordBatch().getRecordCount(), 0);
    setValueCount(recordCount);
    return IterOutcome.OK;
  }


  //todo the same methods in union
  private void setValueCount(int count) {
    for (ValueVector v : allocationVectors) {
      ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }
  }

  //todo the same methods in union
  private boolean doAlloc() {
    for (ValueVector v : allocationVectors) {
      try {
        AllocationHelper.allocateNew(v, validatorInput.getRightRecordBatch().getRecordCount());
      } catch (OutOfMemoryException ex) {
        return false;
      }
    }
    return true;
  }


  //todo do we need it?
  public static class ValidatorInput {
    private final ValidatorRecordBatch validatorRecordBatch;
    private final OneSideInput leftSide;
    private final OneSideInput rightSide;
    private final List<String> columns;

    private List<MaterializedField> outputFields; //todo can be final?
    private IterOutcome upstream = IterOutcome.NOT_YET; //todo rename to status or outcome?
    private boolean rightIsFinish = false;
    BatchSchema rightSchema;
    boolean isStrict = true; //todo replace with configuration parameter (session scope)

    public ValidatorInput(ValidatorRecordBatch validatorRecordBatch, RecordBatch left, RecordBatch right, List<String> columns) {
      this.validatorRecordBatch = validatorRecordBatch;
      this.leftSide = new OneSideInput(left, validatorRecordBatch);
      this.rightSide = new OneSideInput(right, validatorRecordBatch);
      this.columns = columns;
    }

    public IterOutcome nextBatch() throws SchemaChangeException {
      if (upstream == RecordBatch.IterOutcome.NOT_YET) {
        //todo can we do this validation earlier? left side is only checked once
        IterOutcome iterLeft = leftSide.nextBatch();

        // define implicit file columns
        Map<String, ImplicitColumnExplorer.ImplicitFileColumns> implicitColumns =
            ImplicitColumnExplorer.initImplicitFileColumns(validatorRecordBatch.getContext().getOptions());
        switch (iterLeft) {
          case OK_NEW_SCHEMA:
            BatchSchema leftSchema = leftSide.getRecordBatch().getSchema();
            outputFields = Lists.newArrayList();

            //todo we have issues with implicit columns
            /*
            We are expecting select from target to have star query,

             */
            // exclude implicit columns
            for (MaterializedField field : leftSchema) {
              if (implicitColumns.get(field.getName()) != null) {
                continue;
              }
              outputFields.add(field);
            }

            // validate against list of columns indicated in INSERT statement, column names, quantity, order MUST match
            Preconditions.checkState(columns.size() == outputFields.size(), "Columns size in insert should match target column size");
            // todo more descriptive exception, user exception, add columns list
            Iterator<MaterializedField> targetIterator = outputFields.iterator();
            Iterator<String> columnsIterator = columns.iterator();

            while (targetIterator.hasNext() && columnsIterator.hasNext()) {
              if (!targetIterator.next().getName().equalsIgnoreCase(columnsIterator.next())) {
                // todo more description exception, user exception, add columns list
                throw new DrillRuntimeException("Columns names and order in insert should match target column names and order");
              }
            }
            break;
          case STOP:
          case OUT_OF_MEMORY:
            return iterLeft;
          default:
            throw new IllegalStateException(
                String.format("Unexpected state %s.", iterLeft));
        }

        IterOutcome iterRight = rightSide.nextBatch();
        switch (iterRight) {
          case OK_NEW_SCHEMA:
            whileLoop:
            while(rightSide.getRecordBatch().getRecordCount() == 0) {
              iterRight = rightSide.nextBatch();
              switch(iterRight) {
                case STOP:
                case OUT_OF_MEMORY:
                  return iterRight;

                case NONE:
                  // Special Case: The right side was an empty input.
                  rightIsFinish = true;
                  break whileLoop;

                case NOT_YET:
                case OK_NEW_SCHEMA:
                case OK:
                  continue whileLoop;

                default:
                  throw new IllegalStateException(
                      String.format("Unexpected state %s.", iterRight));
              }
            }
            //todo validate right data types can be cast to left

            // validate counts
            BatchSchema rightSchema = rightSide.getRecordBatch().getSchema();

            // if counts between source and target do not match
            // exclude implicit columns from source and check again
            if (outputFields.size() != rightSchema.getFieldCount()) {

            }

            Preconditions.checkState(outputFields.size() == rightSchema.getFieldCount(),
                "Target columns count should match source columns count "); //todo give more descriptive exception
            //can be check this way as well !leftIter.hasNext() && ! rightIter.hasNext(), faster just check counts

            Iterator<MaterializedField> outputIter = outputFields.iterator();
            Iterator<MaterializedField> rightIter = rightSchema.iterator();

            List<MaterializedField> updatedOutputFields = Lists.newArrayList();

            while (outputIter.hasNext() && rightIter.hasNext()) {
              MaterializedField outputField  = outputIter.next();
              MaterializedField rightField = rightIter.next();

              //todo should be two options: strict and non strict
              //todo strict data types should be castable, data mode should match, or be more restrictive (if target optional, we can accept required)
              //todo non-strict data types should be castable, data mode can be less restrictive (if target required, we can accept optional)

              // currently strict mode is used
              if (!hasSameTypeAndMode(outputField, rightField)) {
                // define minor type
                TypeProtos.MinorType outputMinorType = outputField.getType().getMinorType();
                  if (!TypeCastRules.isCastable(rightField.getType().getMinorType(), outputField.getType().getMinorType())) {
                    //todo add proper exception with more details
                    throw new DrillRuntimeException("Type mismatch between target and source");
                }

                // define data mode
                TypeProtos.DataMode dataMode;
                if (isStrict) {
                  dataMode = outputField.getDataMode();
                  if (outputField.getDataMode() == TypeProtos.DataMode.REQUIRED) {
                    Preconditions.checkArgument(rightField.getDataMode() == TypeProtos.DataMode.REQUIRED,
                        "Source should have required data mode as target");
                  } else if (outputField.getDataMode() == TypeProtos.DataMode.REPEATED) {
                    Preconditions.checkArgument(rightField.getDataMode() == TypeProtos.DataMode.REPEATED,
                        "Source should have repeated data mode as target");
                  } else if (outputField.getDataMode() == TypeProtos.DataMode.OPTIONAL) {
                    Preconditions.checkArgument(rightField.getDataMode() == TypeProtos.DataMode.REQUIRED ||
                            rightField.getDataMode() == TypeProtos.DataMode.OPTIONAL,
                        "Source should have required or optional since target is optional");
                  }
                } else {
                  dataMode = TypeProtos.DataMode.REQUIRED;
                  if (outputField.getDataMode() == TypeProtos.DataMode.REPEATED ||
                      outputField.getDataMode() == rightField.getDataMode()) {
                    dataMode = outputField.getDataMode();
                  } else if (rightField.getDataMode() == TypeProtos.DataMode.REPEATED) {
                    //todo proper exception
                    throw new DrillRuntimeException("Target data mode is more restrictive, source data mode is repeated.");
                  } else if (outputField.getDataMode() == TypeProtos.DataMode.OPTIONAL ||
                      outputField.getDataMode() == TypeProtos.DataMode.OPTIONAL) {
                    dataMode = TypeProtos.DataMode.OPTIONAL;
                  }
                }

                // add updated output field
                TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder();
                builder.setMinorType(outputMinorType);
                builder.setMode(dataMode);

                updatedOutputFields.add(MaterializedField.create(outputField.getName(), builder.build()));
                continue;
              }
              updatedOutputFields.add(outputField);
            }
            outputFields = updatedOutputFields;
            break;
          case STOP:
          case OUT_OF_MEMORY:
            return iterRight;

          default:
            throw new IllegalStateException(
                String.format("Unexpected state %s.", iterRight));
        }

        upstream = IterOutcome.OK_NEW_SCHEMA;
        return upstream;
      } else {
        if (rightIsFinish) {
          return IterOutcome.NONE;
        }

        IterOutcome iterOutcome = rightSide.nextBatch();
        switch(iterOutcome) {
          case NONE:
            rightIsFinish = true;
            // fall through
          case STOP:
          case OUT_OF_MEMORY:
            upstream = iterOutcome;
            return upstream;

          case OK_NEW_SCHEMA:
            if (!rightSide.getRecordBatch().getSchema().equals(rightSchema)) {
              throw new SchemaChangeException("Schema change detected in the source input. " +
                  "This is not currently supported"); //todo what we do here in validation case? can we accept schema change?
            }
            iterOutcome = IterOutcome.OK;
            // fall through
          case OK:
            upstream = iterOutcome;
            return upstream;

          default:
            throw new IllegalStateException(String.format("Unknown state %s.", upstream));
        }
      }
    }

    public List<MaterializedField> getOutputFields() {
      if (outputFields == null) {
        throw new NullPointerException("Output fields have not been inferred");
      }
      return outputFields;
    }

    public RecordBatch getLeftRecordBatch() {
      return leftSide.getRecordBatch();
    }

    public RecordBatch getRightRecordBatch() {
      return rightSide.getRecordBatch();
    }

    //todo the same method is in UnionAllRecordBatch, factor out
    public static boolean hasSameTypeAndMode(MaterializedField leftField, MaterializedField rightField) {
      return (leftField.getType().getMinorType() == rightField.getType().getMinorType())
          && (leftField.getType().getMode() == rightField.getType().getMode());
    }

    //todo common with union -> UnionAllRecordBatch // refactor
    private static class OneSideInput {
      private IterOutcome upstream = IterOutcome.NOT_YET;

      private RecordBatch recordBatch;

      private AbstractRecordBatch<?> parentRecordBatch;

      public OneSideInput(RecordBatch recordBatch, AbstractRecordBatch<?> parentRecordBatch) {
        this.recordBatch = recordBatch;
        this.parentRecordBatch = parentRecordBatch;
      }

      public RecordBatch getRecordBatch() {
        return recordBatch;
      }

      public IterOutcome nextBatch() {
        if (upstream == IterOutcome.NONE) {
          throw new IllegalStateException(String.format("Unknown state %s.", upstream));
        }

        if (upstream == IterOutcome.NOT_YET) {
          upstream = parentRecordBatch.next(recordBatch);

          return upstream;
        } else {
          do {
            upstream = parentRecordBatch.next(recordBatch);
          } while (upstream == IterOutcome.OK && recordBatch.getRecordCount() == 0);

          return upstream;
        }
      }
    }
  }
}

