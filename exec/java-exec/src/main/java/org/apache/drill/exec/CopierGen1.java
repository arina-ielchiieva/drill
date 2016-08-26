package org.apache.drill.exec;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.IntVector;

public class CopierGen1 {

    IntVector vv0;
    IntVector vv3;
    IntVector vv6;
    IntVector vv9;
    IntVector vv12;
    IntVector vv15;

    public void doEval(int inIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            vv3 .copyFrom((inIndex), (outIndex), vv0);
        }
        {
            vv9 .copyFrom((inIndex), (outIndex), vv6);
        }
        {
            vv15 .copyFrom((inIndex), (outIndex), vv12);
        }
    }

    public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incoming).getValueAccessorById(IntVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((IntVector) tmp2);
            int[] fieldIds4 = new int[ 1 ] ;
            fieldIds4 [ 0 ] = 0;
            Object tmp5 = (outgoing).getValueAccessorById(IntVector.class, fieldIds4).getValueVector();
            if (tmp5 == null) {
                throw new SchemaChangeException("Failure while loading vector vv3 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv3 = ((IntVector) tmp5);
        }
        {
            int[] fieldIds7 = new int[ 1 ] ;
            fieldIds7 [ 0 ] = 1;
            Object tmp8 = (incoming).getValueAccessorById(IntVector.class, fieldIds7).getValueVector();
            if (tmp8 == null) {
                throw new SchemaChangeException("Failure while loading vector vv6 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv6 = ((IntVector) tmp8);
            int[] fieldIds10 = new int[ 1 ] ;
            fieldIds10 [ 0 ] = 1;
            Object tmp11 = (outgoing).getValueAccessorById(IntVector.class, fieldIds10).getValueVector();
            if (tmp11 == null) {
                throw new SchemaChangeException("Failure while loading vector vv9 with id: TypedFieldId [fieldIds=[1], remainder=null].");
            }
            vv9 = ((IntVector) tmp11);
        }
        {
            int[] fieldIds13 = new int[ 1 ] ;
            fieldIds13 [ 0 ] = 2;
            Object tmp14 = (incoming).getValueAccessorById(IntVector.class, fieldIds13).getValueVector();
            if (tmp14 == null) {
                throw new SchemaChangeException("Failure while loading vector vv12 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv12 = ((IntVector) tmp14);
            int[] fieldIds16 = new int[ 1 ] ;
            fieldIds16 [ 0 ] = 2;
            Object tmp17 = (outgoing).getValueAccessorById(IntVector.class, fieldIds16).getValueVector();
            if (tmp17 == null) {
                throw new SchemaChangeException("Failure while loading vector vv15 with id: TypedFieldId [fieldIds=[2], remainder=null].");
            }
            vv15 = ((IntVector) tmp17);
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
