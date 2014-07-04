package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class ProjectCols extends GenericUDF {

	private ListObjectInspector inloi ;
	private StructObjectInspector insoi ;
	private List<Integer> fieldIndex;

	@Override
	public Object evaluate(DeferredObject[] values) throws HiveException {
		// TODO Auto-generated method stub
        List<?> table = inloi.getList(values[0].get());
        List<Object> outputTable = new ArrayList<Object>();
        for(Object row : table){
        	List<Object> fields = insoi.getStructFieldsDataAsList(row);
        	List<Object> outputFields = new ArrayList<Object>();
        	for(int i : fieldIndex) {
        		outputFields.add(fields.get(i));
        	}
        	outputTable.add(outputFields);
        }
		return outputTable;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] ois)
			throws UDFArgumentException {

		if (! (ois[0] instanceof ListObjectInspector) ||
				!( ((ListObjectInspector)ois[0]).getListElementObjectInspector() instanceof StructObjectInspector ) ) {
			throw new UDFArgumentException("ProjectCol accept array of struct as first argument");
		}

		inloi = (ListObjectInspector) ois[0];
		insoi = (StructObjectInspector) inloi.getListElementObjectInspector();

		List<String> structFieldNames = new ArrayList<String>();
		List<ObjectInspector> structFieldOIs = new ArrayList<ObjectInspector>();

		List<? extends StructField> fields = insoi.getAllStructFieldRefs();
		
		for(int i = 1 ; i < ois.length ; i++){
			
			int index = ((WritableConstantIntObjectInspector)ois[i]).getWritableConstantValue().get();
			fieldIndex.add(index);
			StructField field = fields.get(index);
			structFieldNames.add(field.getFieldName());
			structFieldOIs.add(field.getFieldObjectInspector());			
		}

		StructObjectInspector outsoi = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldOIs);
		
		return ObjectInspectorFactory.getStandardListObjectInspector(outsoi);
	}

}
