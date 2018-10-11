package org.apache.drill.exec;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.UntypedNullHolder;
import org.apache.drill.exec.vector.UntypedNullVector;

public class StreamingAggregatorGen10 {

    UntypedNullVector vv0;
    UntypedNullVector vv4;
    IntHolder constant10;
    UntypedNullVector vv11;
    UntypedNullVector vv15;
    IntHolder constant21;
    UntypedNullVector vv22;
    UntypedNullVector vv26;
    UntypedNullVector vv29;
    UntypedNullVector vv33;

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
            UntypedNullHolder out3 = new UntypedNullHolder();
            {
                out3 .isSet = vv0 .getAccessor().isSet((index1));
                if (out3 .isSet == 1) {
                    vv0 .getAccessor().get((index1), out3);
                }
            }
            UntypedNullHolder out7 = new UntypedNullHolder();
            {
                out7 .isSet = vv4 .getAccessor().isSet((index2));
                if (out7 .isSet == 1) {
                    vv4 .getAccessor().get((index2), out7);
                }
            }
            UntypedNullHolder out8 = new UntypedNullHolder();
            //---- start of eval portion of compare_to_nulls_high function. ----//
            IntHolder out9 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                UntypedNullHolder left = out8;
                UntypedNullHolder right = out8;
                 
GCompareVarBinaryVsVarBinary$GCompareNullableVarBinaryVsNullableVarBinaryNullHigh_eval: {
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
 
                out9 = out;
            }
            //---- end of eval portion of compare_to_nulls_high function. ----//
            constant10 = out9;
            if (constant10 .value!= 0) {
                return false;
            }
            return true;
        }
    }

    public boolean isSamePrev(int b1Index, InternalBatch b1, int b2Index)
        throws SchemaChangeException
    {
        {
            int[] fieldIds12 = new int[ 1 ] ;
            fieldIds12 [ 0 ] = 0;
            Object tmp13 = (b1).getValueAccessorById(UntypedNullVector.class, fieldIds12).getValueVector();
            if (tmp13 == null) {
                throw new SchemaChangeException("Failure while loading vector vv11 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv11 = ((UntypedNullVector) tmp13);
            UntypedNullHolder out14 = new UntypedNullHolder();
            {
                out14 .isSet = vv11 .getAccessor().isSet((b1Index));
                if (out14 .isSet == 1) {
                    vv11 .getAccessor().get((b1Index), out14);
                }
            }
            UntypedNullHolder out18 = new UntypedNullHolder();
            {
                out18 .isSet = vv15 .getAccessor().isSet((b2Index));
                if (out18 .isSet == 1) {
                    vv15 .getAccessor().get((b2Index), out18);
                }
            }
            UntypedNullHolder out19 = new UntypedNullHolder();
            //---- start of eval portion of compare_to_nulls_high function. ----//
            IntHolder out20 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                UntypedNullHolder left = out19;
                UntypedNullHolder right = out19;
                 
GCompareVarBinaryVsVarBinary$GCompareNullableVarBinaryVsNullableVarBinaryNullHigh_eval: {
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
 
                out20 = out;
            }
            //---- end of eval portion of compare_to_nulls_high function. ----//
            constant21 = out20;
            if (constant21 .value!= 0) {
                return false;
            }
            return true;
        }
    }

    public void outputRecordKeys(int inIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            UntypedNullHolder out25 = new UntypedNullHolder();
            {
                out25 .isSet = vv22 .getAccessor().isSet((inIndex));
                if (out25 .isSet == 1) {
                    vv22 .getAccessor().get((inIndex), out25);
                }
            }
            if (!(out25 .isSet == 0)) {
                vv26 .getMutator().setSafe((outIndex), out25 .isSet, out25);
            }
        }
    }

    public void outputRecordKeysPrev(InternalBatch previous, int previousIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            int[] fieldIds30 = new int[ 1 ] ;
            fieldIds30 [ 0 ] = 0;
            Object tmp31 = (previous).getValueAccessorById(UntypedNullVector.class, fieldIds30).getValueVector();
            if (tmp31 == null) {
                throw new SchemaChangeException("Failure while loading vector vv29 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv29 = ((UntypedNullVector) tmp31);
            UntypedNullHolder out32 = new UntypedNullHolder();
            {
                out32 .isSet = vv29 .getAccessor().isSet((previousIndex));
                if (out32 .isSet == 1) {
                    vv29 .getAccessor().get((previousIndex), out32);
                }
            }
            if (!(out32 .isSet == 0)) {
                vv33 .getMutator().setSafe((outIndex), out32 .isSet, out32);
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
            Object tmp2 = (incoming).getValueAccessorById(UntypedNullVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((UntypedNullVector) tmp2);
            int[] fieldIds5 = new int[ 1 ] ;
            fieldIds5 [ 0 ] = 0;
            Object tmp6 = (incoming).getValueAccessorById(UntypedNullVector.class, fieldIds5).getValueVector();
            if (tmp6 == null) {
                throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv4 = ((UntypedNullVector) tmp6);
            /** start SETUP for function compare_to_nulls_high **/ 
  /*          {
                UntypedNullHolder left = out8;
                UntypedNullHolder right = out8;
                 {}
            }*/
            /** end SETUP for function compare_to_nulls_high **/ 
            int[] fieldIds16 = new int[ 1 ] ;
            fieldIds16 [ 0 ] = 0;
            Object tmp17 = (incoming).getValueAccessorById(UntypedNullVector.class, fieldIds16).getValueVector();
            if (tmp17 == null) {
                throw new SchemaChangeException("Failure while loading vector vv15 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv15 = ((UntypedNullVector) tmp17);
            /** start SETUP for function compare_to_nulls_high **/ 
/*            {
                UntypedNullHolder left = out19;
                UntypedNullHolder right = out19;
                 {}
            }*/
            /** end SETUP for function compare_to_nulls_high **/ 
        }
        {
            int[] fieldIds23 = new int[ 1 ] ;
            fieldIds23 [ 0 ] = 0;
            Object tmp24 = (incoming).getValueAccessorById(UntypedNullVector.class, fieldIds23).getValueVector();
            if (tmp24 == null) {
                throw new SchemaChangeException("Failure while loading vector vv22 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv22 = ((UntypedNullVector) tmp24);
            int[] fieldIds27 = new int[ 1 ] ;
            fieldIds27 [ 0 ] = 0;
            Object tmp28 = (outgoing).getValueAccessorById(UntypedNullVector.class, fieldIds27).getValueVector();
            if (tmp28 == null) {
                throw new SchemaChangeException("Failure while loading vector vv26 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv26 = ((UntypedNullVector) tmp28);
        }
        {
            int[] fieldIds34 = new int[ 1 ] ;
            fieldIds34 [ 0 ] = 0;
            Object tmp35 = (outgoing).getValueAccessorById(UntypedNullVector.class, fieldIds34).getValueVector();
            if (tmp35 == null) {
                throw new SchemaChangeException("Failure while loading vector vv33 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv33 = ((UntypedNullVector) tmp35);
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}