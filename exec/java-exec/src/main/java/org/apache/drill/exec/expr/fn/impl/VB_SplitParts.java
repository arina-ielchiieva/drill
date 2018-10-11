package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;


// @FunctionTemplate(name = "vb_split_parts", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
@FunctionTemplate(name = "vb_split_parts", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class VB_SplitParts implements DrillSimpleFunc {
  @Param
  VarCharHolder input;

  @Param
  VarCharHolder delimiter;

  @Workspace
  String splitChar;

  @Inject
  DrillBuf buffer;

  @Output
  org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter writer;

  @Override
  public void setup() {
    splitChar = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.
      toStringFromUTF8(delimiter.start, delimiter.end, delimiter.buffer); //.charAt(0);
  }

  @Override
  public void eval() {
    Iterable<String> tokens = com.google.common.base.Splitter.on(splitChar).split(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer));
    org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = writer.rootAsList();

    list.startList();
    for (String token : tokens) {
      final byte[] strBytes = token.getBytes(com.google.common.base.Charsets.UTF_8);
      buffer = buffer.reallocIfNeeded(strBytes.length);
      buffer.setBytes(0, strBytes);
      list.varChar().writeVarChar(0, strBytes.length, buffer);
    }
    list.endList();
  }
}
