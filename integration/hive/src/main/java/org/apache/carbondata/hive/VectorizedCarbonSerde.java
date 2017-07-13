package org.apache.carbondata.hive;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

public class VectorizedCarbonSerde extends CarbonHiveSerDe {

  private final ArrayWritable[] carbonStructArray = new ArrayWritable [VectorizedRowBatch.DEFAULT_SIZE];
  private final Writable[] rowArray = new Writable [VectorizedRowBatch.DEFAULT_SIZE];
  private final ObjectWritable ow = new ObjectWritable();
  private final ObjectInspector inspector = null;
  private final VectorExpressionWriter[] valueWriters;

  public VectorizedCarbonSerde(ObjectInspector objInspector) {
    super();
    for (int i = 0; i < carbonStructArray.length; i++) {
      rowArray[i] = new CarbonSerdeRow();
    }
    try {
      valueWriters = VectorExpressionWriterFactory
          .getExpressionWriters((StructObjectInspector) objInspector);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector inspector) {
    VectorizedRowBatch batch = (VectorizedRowBatch) obj;
    try {
      for (int i = 0; i < batch.size; i++) {
        ArrayWritable carbonRow = carbonStructArray[i];
        if (carbonRow == null) {
          carbonRow = new ArrayWritable(Writable.class, new Writable[batch.numCols]);
          carbonStructArray[i] = carbonRow;
        }
        int index = 0;
        if (batch.selectedInUse) {
          index = batch.selected[i];
        } else {
          index = i;
        }
        for (int p = 0; p < batch.projectionSize; p++) {
          int k = batch.projectedColumns[p];
          if (batch.cols[k].isRepeating) {
            valueWriters[p].setValue(carbonRow, batch.cols[k], 0);
          } else {
            valueWriters[p].setValue(carbonRow, batch.cols[k], index);
          }
        }
        CarbonSerdeRow row = (CarbonSerdeRow) rowArray[i];
        row.realRow = row;
        row.inspector = inspector;
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
    ow.set(rowArray);
    return ow;
  }

}
