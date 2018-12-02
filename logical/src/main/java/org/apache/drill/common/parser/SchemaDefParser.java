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
package org.apache.drill.common.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.drill.common.exceptions.SchemaParsingException;
import org.apache.drill.common.schema.ColumnDef;
import org.apache.drill.common.schema.SchemaDef;
import org.apache.drill.common.schema.TypeDef;
import org.apache.drill.common.schema.parser.SchemaLexer;
import org.apache.drill.common.schema.parser.SchemaParser;

public class SchemaDefParser {

  public static SchemaDef parseSchema(String schema) {
    SchemaDefVisitor visitor = new SchemaDefVisitor();
    return visitor.visit(initParser(schema).schema());
  }

  public static ColumnDef parseColumn(String column) {
    SchemaDefVisitor.ColumnDefVisitor visitor = new SchemaDefVisitor.ColumnDefVisitor();
    return visitor.visit(initParser(column).column());
  }

  public static TypeDef parseType(String type) {
    SchemaDefVisitor.TypeDefVisitor visitor = new SchemaDefVisitor.TypeDefVisitor();
    return visitor.visit(initParser(type).type_def());
  }

  private static SchemaParser initParser(String value) {
    CodePointCharStream stream = CharStreams.fromString(value);
    CaseChangingCharStream upperCaseStream = new CaseChangingCharStream(stream, true);

    SchemaLexer lexer = new SchemaLexer(upperCaseStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(ErrorListener.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer);

    SchemaParser parser = new SchemaParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ErrorListener.INSTANCE);

    return parser;
  }

  /**
   * Custom error listener that converts all syntax errors into {@link SchemaParsingException}.
   */
  private static class ErrorListener extends BaseErrorListener {

    static final ErrorListener INSTANCE = new ErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
      throw new SchemaParsingException(
        String.format("Line [%s], position [%s]: [%s].", line, charPositionInLine, msg));
    }

  }

}
