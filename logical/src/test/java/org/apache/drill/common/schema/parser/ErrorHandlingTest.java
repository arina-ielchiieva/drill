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
package org.apache.drill.common.schema.parser;

import org.apache.drill.common.exceptions.SchemaParsingException;
import org.apache.drill.common.parser.SchemaDefParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ErrorHandlingTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testIndexedOnlyColumns() {
    String schema = "columns[0] int as indexed_column, named_column varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'named_column' expecting COLUMNS");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testNamedOnlyColumns() {
    String schema = "named_column varchar, columns[0] int as indexed_column";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'columns' expecting {ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnsupportedType() {
    String schema = "col unk_type";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'unk_type' expecting {'INT', 'INTEGER', 'BIGINT'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testVarcharWithScale() {
    String schema = "col varchar(1, 2)";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("[missing ')' at ','");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnquotedKeyword() {
    String schema = "int varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'int' expecting {'(', COLUMNS, ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnquotedId() {
    String schema = "id with space varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'with' expecting {'INT', 'INTEGER', 'BIGINT'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnescapedBackTick() {
    String schema = "`c`o`l` varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'o' expecting {'INT', 'INTEGER', 'BIGINT'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnescapedBackSlash() {
    String schema = "`c\\o\\l` varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input '`' expecting {'(', COLUMNS, ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testNonColumnsKeyword() {
    String schema = "ARRAYS[0] varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input '[' expecting {'INT', 'INTEGER', 'BIGINT'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testMissingType() {
    String schema = "col not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'not' expecting {'INT', 'INTEGER', 'BIGINT'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testIncorrectEOF() {
    String schema = "col int not null footer";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input 'footer' expecting <EOF>");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testSchemaWithOneParen() {
    String schema = "(col int not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing ')' at '<EOF>'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testMissingAngleBracket() {
    String schema = "col array<int not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing '>' at 'not'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testUnclosedAngleBracket() {
    String schema = "col map<m array<int> not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing '>' at '<EOF>'");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testMissingColumnNameForMap() {
    String schema = "col map<int> not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'int' expecting {ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testMissingNotBeforeNull() {
    String schema = "col int null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input 'null' expecting <EOF>");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testExtraComma() {
    String schema = "id int,, name varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input ',' expecting {ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void testExtraCommaEOF() {
    String schema = "id int, name varchar,";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input '<EOF>' expecting {ID, QUOTED_ID}");
    SchemaDefParser.parseSchema(schema);
  }

  @Test
  public void incorrectNumber() {
    String schema = "id decimal(5, 02)";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input '2' expecting ')'");
    SchemaDefParser.parseSchema(schema);
  }

}
