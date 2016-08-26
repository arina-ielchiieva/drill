package org.apache.drill.exec;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;

public class StreamingAggregatorGen0 {

    BigIntHolder work0;
    BigIntHolder work1;
    NullableBigIntVector vv2;
    NullableBigIntVector vv7;
    ObjectHolder work10;
    UInt1Holder work11;
    BigIntHolder work12;
    DrillBuf work13;
    NullableVarCharVector vv14;
    NullableVarCharVector vv19;
    ObjectHolder work22;
    UInt1Holder work23;
    BigIntHolder work24;
    DrillBuf work25;
    NullableVarCharVector vv26;
    NullableVarCharVector vv31;

    public void outputRecordKeys(int inIndex, int outIndex)
        throws SchemaChangeException
    {
    }

    public void outputRecordKeysPrev(InternalBatch previous, int previousIndex, int outIndex)
        throws SchemaChangeException
    {
    }

    public void setupInterior(RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            /** start SETUP for function max **/ 
            {
                BigIntHolder value = work0;
                BigIntHolder nonNullCount = work1;
                 
MaxFunctions$NullableBigIntMax_setup: {
    value = new BigIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    value.value = Long.MIN_VALUE;
}
 
                work0 = value;
                work1 = nonNullCount;
            }
            /** end SETUP for function max **/ 
            int[] fieldIds3 = new int[ 1 ] ;
            fieldIds3 [ 0 ] = 0;
            Object tmp4 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds3).getValueVector();
            if (tmp4 == null) {
                throw new SchemaChangeException("Failure while loading vector vv2 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv2 = ((NullableBigIntVector) tmp4);
            int[] fieldIds8 = new int[ 1 ] ;
            fieldIds8 [ 0 ] = 0;
            Object tmp9 = (outgoing).getValueAccessorById(NullableBigIntVector.class, fieldIds8).getValueVector();
            if (tmp9 == null) {
                throw new SchemaChangeException("Failure while loading vector vv7 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv7 = ((NullableBigIntVector) tmp9);
        }
        {
            work13 = (incoming).getContext().getManagedBuffer();
            /** start SETUP for function max **/ 
            {
                ObjectHolder value = work10;
                UInt1Holder init = work11;
                BigIntHolder nonNullCount = work12;
                DrillBuf buf = work13;
                 
MaxVarBytesFunctions$NullableVarCharMax_setup: {
    init = new UInt1Holder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    init.value = 0;
    value = new ObjectHolder();

    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();

    value.obj = tmp;
}
 
                work10 = value;
                work11 = init;
                work12 = nonNullCount;
                work13 = buf;
            }
            /** end SETUP for function max **/ 
            int[] fieldIds15 = new int[ 1 ] ;
            fieldIds15 [ 0 ] = 1;
            Object tmp16 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds15).getValueVector();
            if (tmp16 == null) {
                throw new SchemaChangeException("Failure while loading vector vv14 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv14 = ((NullableVarCharVector) tmp16);
            int[] fieldIds20 = new int[ 1 ] ;
            fieldIds20 [ 0 ] = 1;
            Object tmp21 = (outgoing).getValueAccessorById(NullableVarCharVector.class, fieldIds20).getValueVector();
            if (tmp21 == null) {
                throw new SchemaChangeException("Failure while loading vector vv19 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv19 = ((NullableVarCharVector) tmp21);
        }
        {
            work25 = (incoming).getContext().getManagedBuffer();
            /** start SETUP for function max **/ 
            {
                ObjectHolder value = work22;
                UInt1Holder init = work23;
                BigIntHolder nonNullCount = work24;
                DrillBuf buf = work25;
                 
MaxVarBytesFunctions$NullableVarCharMax_setup: {
    init = new UInt1Holder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    init.value = 0;
    value = new ObjectHolder();

    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();

    value.obj = tmp;
}
 
                work22 = value;
                work23 = init;
                work24 = nonNullCount;
                work25 = buf;
            }
            /** end SETUP for function max **/ 
            int[] fieldIds27 = new int[ 1 ] ;
            fieldIds27 [ 0 ] = 2;
            Object tmp28 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds27).getValueVector();
            if (tmp28 == null) {
                throw new SchemaChangeException("Failure while loading vector vv26 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv26 = ((NullableVarCharVector) tmp28);
            int[] fieldIds32 = new int[ 1 ] ;
            fieldIds32 [ 0 ] = 2;
            Object tmp33 = (outgoing).getValueAccessorById(NullableVarCharVector.class, fieldIds32).getValueVector();
            if (tmp33 == null) {
                throw new SchemaChangeException("Failure while loading vector vv31 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv31 = ((NullableVarCharVector) tmp33);
        }
    }

    public boolean isSame(int index1, int index2)
        throws SchemaChangeException
    {
        {
            return true;
        }
    }

    public boolean isSamePrev(int b1Index, InternalBatch b1, int b2Index)
        throws SchemaChangeException
    {
        {
            return true;
        }
    }

    public void outputRecordValues(int outIndex)
        throws SchemaChangeException
    {
        {
            NullableBigIntHolder out6;
            {
                final NullableBigIntHolder out = new NullableBigIntHolder();
                BigIntHolder value = work0;
                BigIntHolder nonNullCount = work1;
                 
MaxFunctions$NullableBigIntMax_output: {
    if (nonNullCount.value > 0) {
        out.value = value.value;
        out.isSet = 1;
    } else
    {
        out.isSet = 0;
    }
}
 
                work0 = value;
                work1 = nonNullCount;
                out6 = out;
            }
            if (!(out6 .isSet == 0)) {
                vv7 .getMutator().setSafe((outIndex), out6 .isSet, out6 .value);
            }
        }
        {
            NullableVarCharHolder out18;
            {
                final NullableVarCharHolder out = new NullableVarCharHolder();
                ObjectHolder value = work10;
                UInt1Holder init = work11;
                BigIntHolder nonNullCount = work12;
                DrillBuf buf = work13;
                 
MaxVarBytesFunctions$NullableVarCharMax_output: {
    if (nonNullCount.value > 0) {
        out.isSet = 1;

        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;

        buf = buf.reallocIfNeeded(tmp.getLength());
        buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
        out.start = 0;
        out.end = tmp.getLength();
        out.buffer = buf;
    } else
    {
        out.isSet = 0;
    }
}
 
                work10 = value;
                work11 = init;
                work12 = nonNullCount;
                work13 = buf;
                out18 = out;
            }
            if (!(out18 .isSet == 0)) {
                vv19 .getMutator().setSafe((outIndex), out18 .isSet, out18 .start, out18 .end, out18 .buffer);
            }
        }
        {
            NullableVarCharHolder out30;
            {
                final NullableVarCharHolder out = new NullableVarCharHolder();
                ObjectHolder value = work22;
                UInt1Holder init = work23;
                BigIntHolder nonNullCount = work24;
                DrillBuf buf = work25;
                 
MaxVarBytesFunctions$NullableVarCharMax_output: {
    if (nonNullCount.value > 0) {
        out.isSet = 1;

        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;

        buf = buf.reallocIfNeeded(tmp.getLength());
        buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
        out.start = 0;
        out.end = tmp.getLength();
        out.buffer = buf;
    } else
    {
        out.isSet = 0;
    }
}
 
                work22 = value;
                work23 = init;
                work24 = nonNullCount;
                work25 = buf;
                out30 = out;
            }
            if (!(out30 .isSet == 0)) {
                vv31 .getMutator().setSafe((outIndex), out30 .isSet, out30 .start, out30 .end, out30 .buffer);
            }
        }
    }

    public boolean resetValues()
        throws SchemaChangeException
    {
        {
            /** start RESET for function max **/ 
            {
                BigIntHolder value = work0;
                BigIntHolder nonNullCount = work1;
                 
MaxFunctions$NullableBigIntMax_reset: {
    nonNullCount.value = 0;
    value.value = Long.MIN_VALUE;
}
 
                work0 = value;
                work1 = nonNullCount;
            }
            /** end RESET for function max **/ 
        }
        {
            /** start RESET for function max **/ 
            {
                ObjectHolder value = work10;
                UInt1Holder init = work11;
                BigIntHolder nonNullCount = work12;
                DrillBuf buf = work13;
                 
MaxVarBytesFunctions$NullableVarCharMax_reset: {
    value = new ObjectHolder();
    value.obj = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
    init.value = 0;
    nonNullCount.value = 0;
}
 
                work10 = value;
                work11 = init;
                work12 = nonNullCount;
                work13 = buf;
            }
            /** end RESET for function max **/ 
        }
        {
            /** start RESET for function max **/ 
            {
                ObjectHolder value = work22;
                UInt1Holder init = work23;
                BigIntHolder nonNullCount = work24;
                DrillBuf buf = work25;
                 
MaxVarBytesFunctions$NullableVarCharMax_reset: {
    value = new ObjectHolder();
    value.obj = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
    init.value = 0;
    nonNullCount.value = 0;
}
 
                work22 = value;
                work23 = init;
                work24 = nonNullCount;
                work25 = buf;
            }
            /** end RESET for function max **/ 
            return true;
        }
    }

    public int getVectorIndex(int recordIndex)
        throws SchemaChangeException
    {
        {
            return (recordIndex);
        }
    }

    public void addRecord(int index)
        throws SchemaChangeException
    {
        {
            NullableBigIntHolder out5 = new NullableBigIntHolder();
            {
                out5 .isSet = vv2 .getAccessor().isSet((index));
                if (out5 .isSet == 1) {
                    out5 .value = vv2 .getAccessor().get((index));
                }
            }
            NullableBigIntHolder in = out5;
            BigIntHolder value = work0;
            BigIntHolder nonNullCount = work1;
             
MaxFunctions$NullableBigIntMax_add: {
    sout:
    {
        if (in.isSet == 0) {
            break sout;
        }
        nonNullCount.value = 1;
        value.value = Math.max(value.value, in.value);
    }
}
 
            work0 = value;
            work1 = nonNullCount;
        }
        {
            NullableVarCharHolder out17 = new NullableVarCharHolder();
            {
                out17 .isSet = vv14 .getAccessor().isSet((index));
                if (out17 .isSet == 1) {
                    out17 .buffer = vv14 .getBuffer();
                    long startEnd = vv14 .getAccessor().getStartEnd((index));
                    out17 .start = ((int) startEnd);
                    out17 .end = ((int)(startEnd >> 32));
                }
            }
            NullableVarCharHolder in = out17;
            ObjectHolder value = work10;
            UInt1Holder init = work11;
            BigIntHolder nonNullCount = work12;
            DrillBuf buf = work13;
             
MaxVarBytesFunctions$NullableVarCharMax_add: {
    sout:
    {
        if (in.isSet == 0) {
            break sout;
        }
        nonNullCount.value = 1;

        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
        int cmp = 0;
        boolean swap = false;

        if (init.value == 0) {
            init.value = 1;
            swap = true;
        } else
        {
            cmp = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
                in.buffer,
                in.start,
                in.end,
                tmp.getBytes(),
                0,
                tmp.getLength()
            );
            swap = (cmp == 1);
        }
        if (swap) {
            int inputLength = in.end - in.start;

            if (tmp.getLength() >= inputLength) {
                in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
                tmp.setLength(inputLength);
            } else
            {
                byte[] tempArray = new byte[in.end - in.start];

                in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
                tmp.setBytes(tempArray);
            }
        }
    }
}
 
            work10 = value;
            work11 = init;
            work12 = nonNullCount;
            work13 = buf;
        }
        {
            NullableVarCharHolder out29 = new NullableVarCharHolder();
            {
                out29 .isSet = vv26 .getAccessor().isSet((index));
                if (out29 .isSet == 1) {
                    out29 .buffer = vv26 .getBuffer();
                    long startEnd = vv26 .getAccessor().getStartEnd((index));
                    out29 .start = ((int) startEnd);
                    out29 .end = ((int)(startEnd >> 32));
                }
            }
            NullableVarCharHolder in = out29;
            ObjectHolder value = work22;
            UInt1Holder init = work23;
            BigIntHolder nonNullCount = work24;
            DrillBuf buf = work25;
             
MaxVarBytesFunctions$NullableVarCharMax_add: {
    sout:
    {
        if (in.isSet == 0) {
            break sout;
        }
        nonNullCount.value = 1;

        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
        int cmp = 0;
        boolean swap = false;

        if (init.value == 0) {
            init.value = 1;
            swap = true;
        } else
        {
            cmp = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
                in.buffer,
                in.start,
                in.end,
                tmp.getBytes(),
                0,
                tmp.getLength()
            );
            swap = (cmp == 1);
        }
        if (swap) {
            int inputLength = in.end - in.start;

            if (tmp.getLength() >= inputLength) {
                in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
                tmp.setLength(inputLength);
            } else
            {
                byte[] tempArray = new byte[in.end - in.start];

                in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
                tmp.setBytes(tempArray);
            }
        }
    }
}
 
            work22 = value;
            work23 = init;
            work24 = nonNullCount;
            work25 = buf;
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
        {
            work0 = new BigIntHolder();
            work1 = new BigIntHolder();
        }
        {
            work12 = new BigIntHolder();
        }
        {
            work24 = new BigIntHolder();
        }
    }

}
