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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.schema.parser.SchemaParser;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.parser.TupleSchemaParser;
import org.apache.drill.exec.record.metadata.parser.TupleSchemaParserBaseVisitor;

import java.util.List;
import java.util.stream.Collectors;

//todo remove fqn when parsers is removed from logical package
public class TupleSchemaVisitor extends TupleSchemaParserBaseVisitor<TupleSchema> {

  @Override
  public TupleSchema visitSchema(TupleSchemaParser.SchemaContext ctx) {
    TupleSchema schema = new TupleSchema();
    ctx.columns().accept(new ColumnsVisitor()).forEach(schema::add);
    return schema;
  }

  public static class ColumnsVisitor extends TupleSchemaParserBaseVisitor<List<MaterializedField>> {

    @Override
    public List<MaterializedField> visitColumns(TupleSchemaParser.ColumnsContext ctx) {
      ColumnVisitor columnVisitor = new ColumnVisitor();
      return ctx.column().stream()
        .map(c -> c.accept(columnVisitor))
        .collect(Collectors.toList());
    }
  }

  public static class ColumnVisitor extends TupleSchemaParserBaseVisitor<MaterializedField> {

    @Override
    public MaterializedField visitColumn(TupleSchemaParser.ColumnContext ctx) {
      TupleSchemaParser.Column_attrContext column_attrContext = ctx.column_attr();
      TupleSchemaParser.NullabilityContext nullability = column_attrContext.nullability();
      boolean nullable = nullability == null;
      String name = ctx.column_id().accept(new NameVisitor());

      //MaterializedField.create()
      return super.visitColumn(ctx);
    }
  }

  public static class NameVisitor extends TupleSchemaParserBaseVisitor<String> {

    @Override
    public String visitId(SchemaParser.IdContext ctx) {
      return ctx.ID().getText();
    }

    @Override
    public String visitQuoted_id(SchemaParser.Quoted_idContext ctx) {
      String text = ctx.QUOTED_ID().getText();
      // first substring first and last symbols (backticks)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }

  }

}
