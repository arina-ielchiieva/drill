parser grammar TupleSchemaParser;

options {
  language=Java;
  tokenVocab=TupleSchemaLexer;
}

@header {
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
}

schema: (columns | LEFT_PAREN columns RIGHT_PAREN) EOF;

columns: column (COMMA column)*;

column: column_id column_attr;

column_id
: ID # id
| QUOTED_ID # quoted_id
;

column_attr: type_def nullability?;

type_def
: (INT | INTEGER) # int
| BIGINT # bigint
| FLOAT # float
| DOUBLE # double
| (DEC | DECIMAL | NUMERIC) (LEFT_PAREN NUMBER (COMMA NUMBER)? RIGHT_PAREN)? # decimal
| BOOLEAN # boolean
| (CHAR | VARCHAR | CHARACTER VARYING?) (LEFT_PAREN NUMBER RIGHT_PAREN)? # varchar
| (BINARY | VARBINARY) (LEFT_PAREN NUMBER RIGHT_PAREN)? # binary
| TIME # time
| DATE # date
| TIMESTAMP # timestamp
| INTERVAL # interval
| array_def # array
| map_def # map
;

array_def: ARRAY LEFT_ANGLE_BRACKET type_def RIGHT_ANGLE_BRACKET;

map_def: MAP LEFT_ANGLE_BRACKET columns RIGHT_ANGLE_BRACKET;

nullability: NOT NULL;

