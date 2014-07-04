package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class FilterRows extends GenericUDF {


	private ListObjectInspector inloi ;
	private StructObjectInspector insoi ;
	private StandardConstantListObjectInspector filterIndexsoi ;
	private StructObjectInspector filterValuesoi ;

	List<IntWritable> filterFieldIndex;
	List<Object> filterFieldValue;

	@Override
	public Object evaluate(DeferredObject[] values) throws HiveException {
		// TODO Auto-generated method stub
        List<?> table = inloi.getList(values[0].get());
        List<Object> outputTable = new ArrayList<Object>();
        for(Object row : table){
        	List<Object> fields = insoi.getStructFieldsDataAsList(row);
        	List<Object> outputFields = new ArrayList<Object>();
        	for(int i = 1; i < values.length ; i ++){
        		outputFields.add(fields.get( ((IntWritable)values[i].get()).get()) );
        	}
        	outputTable.add(outputFields);
        }
		return outputTable;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return "filter table rows";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] ois)
			throws UDFArgumentException {
		
		if ( ois.length != 3 || ! (ois[0] instanceof ListObjectInspector) ||
				!( ((ListObjectInspector)ois[0]).getListElementObjectInspector() instanceof StructObjectInspector ) ||
				!( ois[1] instanceof StructObjectInspector )  ||
				!( ois[2] instanceof StructObjectInspector ) ) {
			throw new UDFArgumentException("ProjectCol accept three arguments , and the first one is array of struct, the left must be struct ");
		}

		inloi = (ListObjectInspector) ois[0];
		insoi = (StructObjectInspector) inloi.getListElementObjectInspector();

		filterIndexsoi = (StandardConstantListObjectInspector) ois[1];
		filterFieldIndex = (List<IntWritable>) filterIndexsoi.getWritableConstantValue();

		filterValuesoi = (StructObjectInspector) ois[2];

		return inloi;

	}


}
