package org.apache.drill.exec;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;

public class ProjectorGen0 {

    NullableBigIntVector vv0;
    IntVector vv5;
    NullableVarCharVector vv8;
    IntVector vv13;
    NullableVarCharVector vv16;
    IntVector vv21;

    public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((NullableBigIntVector) tmp2);
            /** start SETUP for function hash **/ 
            {
                 {}
            }
            /** end SETUP for function hash **/ 
            int[] fieldIds6 = new int[ 1 ] ;
            fieldIds6 [ 0 ] = 0;
            Object tmp7 = (outgoing).getValueAccessorById(IntVector.class, fieldIds6).getValueVector();
            if (tmp7 == null) {
                throw new SchemaChangeException("Failure while loading vector vv5 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv5 = ((IntVector) tmp7);
            int[] fieldIds9 = new int[ 1 ] ;
            fieldIds9 [ 0 ] = 1;
            Object tmp10 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds9).getValueVector();
            if (tmp10 == null) {
                throw new SchemaChangeException("Failure while loading vector vv8 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv8 = ((NullableVarCharVector) tmp10);
            /** start SETUP for function hash **/ 
            {
                 {}
            }
            /** end SETUP for function hash **/ 
            int[] fieldIds14 = new int[ 1 ] ;
            fieldIds14 [ 0 ] = 1;
            Object tmp15 = (outgoing).getValueAccessorById(IntVector.class, fieldIds14).getValueVector();
            if (tmp15 == null) {
                throw new SchemaChangeException("Failure while loading vector vv13 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv13 = ((IntVector) tmp15);
            int[] fieldIds17 = new int[ 1 ] ;
            fieldIds17 [ 0 ] = 2;
            Object tmp18 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds17).getValueVector();
            if (tmp18 == null) {
                throw new SchemaChangeException("Failure while loading vector vv16 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv16 = ((NullableVarCharVector) tmp18);
            /** start SETUP for function hash **/ 
            {
                 {}
            }
            /** end SETUP for function hash **/ 
            int[] fieldIds22 = new int[ 1 ] ;
            fieldIds22 [ 0 ] = 2;
            Object tmp23 = (outgoing).getValueAccessorById(IntVector.class, fieldIds22).getValueVector();
            if (tmp23 == null) {
                throw new SchemaChangeException("Failure while loading vector vv21 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv21 = ((IntVector) tmp23);
        }
    }

    public void doEval(int inIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            NullableBigIntHolder out3 = new NullableBigIntHolder();
            {
                out3 .isSet = vv0 .getAccessor().isSet((inIndex));
                if (out3 .isSet == 1) {
                    out3 .value = vv0 .getAccessor().get((inIndex));
                }
            }
            //---- start of eval portion of hash function. ----//
            IntHolder out4 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                NullableBigIntHolder in = out3;
                 
Hash32Functions$NullableBigIntHash_eval: {
    if (in.isSet == 0) {
        out.value = 0;
    } else
    {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
    }
}
 
                out4 = out;
            }
            //---- end of eval portion of hash function. ----//
            vv5 .getMutator().set((outIndex), out4 .value);
            NullableVarCharHolder out11 = new NullableVarCharHolder();
            {
                out11 .isSet = vv8 .getAccessor().isSet((inIndex));
                if (out11 .isSet == 1) {
                    out11 .buffer = vv8 .getBuffer();
                    long startEnd = vv8 .getAccessor().getStartEnd((inIndex));
                    out11 .start = ((int) startEnd);
                    out11 .end = ((int)(startEnd >> 32));
                }
            }
            //---- start of eval portion of hash function. ----//
            IntHolder out12 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                NullableVarCharHolder in = out11;
                 
Hash32Functions$NullableVarCharHash_eval: {
    if (in.isSet == 0) {
        out.value = 0;
    } else
    {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.start, in.end, in.buffer, 0);
    }
}
 
                out12 = out;
            }
            //---- end of eval portion of hash function. ----//
            vv13 .getMutator().set((outIndex), out12 .value);
            NullableVarCharHolder out19 = new NullableVarCharHolder();
            {
                out19 .isSet = vv16 .getAccessor().isSet((inIndex));
                if (out19 .isSet == 1) {
                    out19 .buffer = vv16 .getBuffer();
                    long startEnd = vv16 .getAccessor().getStartEnd((inIndex));
                    out19 .start = ((int) startEnd);
                    out19 .end = ((int)(startEnd >> 32));
                }
            }
            //---- start of eval portion of hash function. ----//
            IntHolder out20 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                NullableVarCharHolder in = out19;
                 
Hash32Functions$NullableVarCharHash_eval: {
    if (in.isSet == 0) {
        out.value = 0;
    } else
    {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.start, in.end, in.buffer, 0);
    }
}
 
                out20 = out;
            }
            //---- end of eval portion of hash function. ----//
            vv21 .getMutator().set((outIndex), out20 .value);
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
