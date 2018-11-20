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
package org.apache.drill.common.expression;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.junit.Test;

public class SchemaExpressionTest {

  @Test
  public void testParser() {
    parse("(`t` int NoT NulL)");
    //parse("`t` int NoT NulL");
    parse("(`t` int)");
  }

  @Test
  public void testException() {
    parse("cast(s as i)");
  }

  @Test
  public void testComments() {
    parse("cast(1 /* aaa */ //hello" +
      "as int) //abc");
  }

  private void parse(String value) {
    ExprLexer lexer = new ExprLexer(CharStreams.fromString(value));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ErrorListener.INSTANCE);


    CommonTokenStream tokens = new CommonTokenStream(lexer);

    ExprParser parser = new ExprParser(tokens);
    parser.removeErrorListeners(); // need to remove since default listener will output warning
    parser.addErrorListener(ErrorListener.INSTANCE);
    //parser.setErrorHandler(new BailErrorStrategy());
    //ParserATNSimulator interpreter = parser.getInterpreter();
    //interpreter.setPredictionMode(PredictionMode.SLL); // prediction mode
    //System.out.println(interpreter.getPredictionMode());

    LogicalExpression expression = parser.parse().e;
    System.out.println(expression);
  }

  public static class ErrorListener extends BaseErrorListener {

   public static final ErrorListener INSTANCE = new ErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg, RecognitionException e) {
      throw new ExpressionParsingException(msg);
    }
  }

  /*
  upgrade issues:
  1. exception handling
  2. remove dependency from common
  -- 3. greedy block
  [WARNING] warning(131): org/apache/drill/common/expression/parser/ExprLexer.g4:138:11: greedy block ()* contains wildcard; the non-greedy syntax ()*? may be preferred
[WARNING] /Users/arina/Development/git_repo/drill/logical/org/apache/drill/common/expression/parser/ExprLexer.g4 [138:11]: greedy block ()* contains wildcard; the non-greedy syntax ()*? may be preferred
   */

}
