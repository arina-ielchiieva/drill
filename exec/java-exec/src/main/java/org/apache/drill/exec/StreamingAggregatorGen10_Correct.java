package org.apache.drill.exec;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.NullableVarCharVector;

public class StreamingAggregatorGen10_Correct {

    NullableVarCharVector vv0;
    NullableVarCharVector vv4;
    NullableVarCharVector vv9;
    NullableVarCharVector vv13;
    NullableVarCharVector vv18;
    NullableVarCharVector vv22;
    NullableVarCharVector vv25;
    NullableVarCharVector vv29;

    public void addRecord(int index)
        throws SchemaChangeException
    {
    }

    public int getVectorIndex(int recordIndex)
        throws SchemaChangeException
    {
        {
            return (recordIndex);
        }
    }

    public boolean isSame(int index1, int index2)
        throws SchemaChangeException
    {
        {
            NullableVarCharHolder out3 = new NullableVarCharHolder();
            {
                out3 .isSet = vv0 .getAccessor().isSet((index1));
                if (out3 .isSet == 1) {
                    out3 .buffer = vv0 .getBuffer();
                    long startEnd = vv0 .getAccessor().getStartEnd((index1));
                    out3 .start = ((int) startEnd);
                    out3 .end = ((int)(startEnd >> 32));
                }
            }
            NullableVarCharHolder out7 = new NullableVarCharHolder();
            {
                out7 .isSet = vv4 .getAccessor().isSet((index2));
                if (out7 .isSet == 1) {
                    out7 .buffer = vv4 .getBuffer();
                    long startEnd = vv4 .getAccessor().getStartEnd((index2));
                    out7 .start = ((int) startEnd);
                    out7 .end = ((int)(startEnd >> 32));
                }
            }
            //---- start of eval portion of compare_to_nulls_high function. ----//
            IntHolder out8 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                NullableVarCharHolder left = out3;
                NullableVarCharHolder right = out7;
                 
GCompareVarCharVsVarChar$GCompareNullableVarCharVsNullableVarCharNullHigh_eval: {
    outside:
    {
        if (left.isSet == 0) {
            if (right.isSet == 0) {
                out.value = 0;
                break outside;
            } else
            {
                out.value = 1;
                break outside;
            }
        } else
        if (right.isSet == 0) {
            out.value = -1;
            break outside;
        }
        out.value = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
            left.buffer,
            left.start,
            left.end,
            right.buffer,
            right.start,
            right.end
        );
    }
}
 
                out8 = out;
            }
            //---- end of eval portion of compare_to_nulls_high function. ----//
            if (out8 .value!= 0) {
                return false;
            }
            return true;
        }
    }

    public boolean isSamePrev(int b1Index, InternalBatch b1, int b2Index)
        throws SchemaChangeException
    {
        {
            int[] fieldIds10 = new int[ 1 ] ;
            fieldIds10 [ 0 ] = 0;
            Object tmp11 = (b1).getValueAccessorById(NullableVarCharVector.class, fieldIds10).getValueVector();
            if (tmp11 == null) {
                throw new SchemaChangeException("Failure while loading vector vv9 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv9 = ((NullableVarCharVector) tmp11);
            NullableVarCharHolder out12 = new NullableVarCharHolder();
            {
                out12 .isSet = vv9 .getAccessor().isSet((b1Index));
                if (out12 .isSet == 1) {
                    out12 .buffer = vv9 .getBuffer();
                    long startEnd = vv9 .getAccessor().getStartEnd((b1Index));
                    out12 .start = ((int) startEnd);
                    out12 .end = ((int)(startEnd >> 32));
                }
            }
            NullableVarCharHolder out16 = new NullableVarCharHolder();
            {
                out16 .isSet = vv13 .getAccessor().isSet((b2Index));
                if (out16 .isSet == 1) {
                    out16 .buffer = vv13 .getBuffer();
                    long startEnd = vv13 .getAccessor().getStartEnd((b2Index));
                    out16 .start = ((int) startEnd);
                    out16 .end = ((int)(startEnd >> 32));
                }
            }
            //---- start of eval portion of compare_to_nulls_high function. ----//
            IntHolder out17 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                NullableVarCharHolder left = out12;
                NullableVarCharHolder right = out16;
                 
GCompareVarCharVsVarChar$GCompareNullableVarCharVsNullableVarCharNullHigh_eval: {
    outside:
    {
        if (left.isSet == 0) {
            if (right.isSet == 0) {
                out.value = 0;
                break outside;
            } else
            {
                out.value = 1;
                break outside;
            }
        } else
        if (right.isSet == 0) {
            out.value = -1;
            break outside;
        }
        out.value = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
            left.buffer,
            left.start,
            left.end,
            right.buffer,
            right.start,
            right.end
        );
    }
}
 
                out17 = out;
            }
            //---- end of eval portion of compare_to_nulls_high function. ----//
            if (out17 .value!= 0) {
                return false;
            }
            return true;
        }
    }

    public void outputRecordKeys(int inIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            NullableVarCharHolder out21 = new NullableVarCharHolder();
            {
                out21 .isSet = vv18 .getAccessor().isSet((inIndex));
                if (out21 .isSet == 1) {
                    out21 .buffer = vv18 .getBuffer();
                    long startEnd = vv18 .getAccessor().getStartEnd((inIndex));
                    out21 .start = ((int) startEnd);
                    out21 .end = ((int)(startEnd >> 32));
                }
            }
            if (!(out21 .isSet == 0)) {
                vv22 .getMutator().setSafe((outIndex), out21 .isSet, out21 .start, out21 .end, out21 .buffer);
            }
        }
    }

    public void outputRecordKeysPrev(InternalBatch previous, int previousIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            int[] fieldIds26 = new int[ 1 ] ;
            fieldIds26 [ 0 ] = 0;
            Object tmp27 = (previous).getValueAccessorById(NullableVarCharVector.class, fieldIds26).getValueVector();
            if (tmp27 == null) {
                throw new SchemaChangeException("Failure while loading vector vv25 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv25 = ((NullableVarCharVector) tmp27);
            NullableVarCharHolder out28 = new NullableVarCharHolder();
            {
                out28 .isSet = vv25 .getAccessor().isSet((previousIndex));
                if (out28 .isSet == 1) {
                    out28 .buffer = vv25 .getBuffer();
                    long startEnd = vv25 .getAccessor().getStartEnd((previousIndex));
                    out28 .start = ((int) startEnd);
                    out28 .end = ((int)(startEnd >> 32));
                }
            }
            if (!(out28 .isSet == 0)) {
                vv29 .getMutator().setSafe((outIndex), out28 .isSet, out28 .start, out28 .end, out28 .buffer);
            }
        }
    }

    public void outputRecordValues(int outIndex)
        throws SchemaChangeException
    {
    }

    public boolean resetValues()
        throws SchemaChangeException
    {
        {
            return true;
        }
    }

    public void setupInterior(RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((NullableVarCharVector) tmp2);
            int[] fieldIds5 = new int[ 1 ] ;
            fieldIds5 [ 0 ] = 0;
            Object tmp6 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds5).getValueVector();
            if (tmp6 == null) {
                throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv4 = ((NullableVarCharVector) tmp6);
            /** start SETUP for function compare_to_nulls_high **/ 
            {
                 {}
            }
            /** end SETUP for function compare_to_nulls_high **/ 
            int[] fieldIds14 = new int[ 1 ] ;
            fieldIds14 [ 0 ] = 0;
            Object tmp15 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds14).getValueVector();
            if (tmp15 == null) {
                throw new SchemaChangeException("Failure while loading vector vv13 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv13 = ((NullableVarCharVector) tmp15);
            /** start SETUP for function compare_to_nulls_high **/ 
            {
                 {}
            }
            /** end SETUP for function compare_to_nulls_high **/ 
        }
        {
            int[] fieldIds19 = new int[ 1 ] ;
            fieldIds19 [ 0 ] = 0;
            Object tmp20 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds19).getValueVector();
            if (tmp20 == null) {
                throw new SchemaChangeException("Failure while loading vector vv18 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv18 = ((NullableVarCharVector) tmp20);
            int[] fieldIds23 = new int[ 1 ] ;
            fieldIds23 [ 0 ] = 0;
            Object tmp24 = (outgoing).getValueAccessorById(NullableVarCharVector.class, fieldIds23).getValueVector();
            if (tmp24 == null) {
                throw new SchemaChangeException("Failure while loading vector vv22 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv22 = ((NullableVarCharVector) tmp24);
        }
        {
            int[] fieldIds30 = new int[ 1 ] ;
            fieldIds30 [ 0 ] = 0;
            Object tmp31 = (outgoing).getValueAccessorById(NullableVarCharVector.class, fieldIds30).getValueVector();
            if (tmp31 == null) {
                throw new SchemaChangeException("Failure while loading vector vv29 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv29 = ((NullableVarCharVector) tmp31);
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
