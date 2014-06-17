package com.autonavi.data.hive;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ComplexDecrptClientLogLine {

	/*
	
	private ArrayList ret;
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return "decrpt client log";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		 if( arguments.length != 1 )
	          throw new UDFArgumentLengthException("DecrptClientLog accepts exactly one argument.");
	 
		 
		      PrimitiveObjectInspector lineOI = ((PrimitiveObjectInspector)arguments[0]);
	          // Is the input an array<>
	          if( lineOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)
	          throw new UDFArgumentTypeException(0,"The single argument to DecrptClientLog should be "
	          + "String"
	          + " but " + arguments[0].getTypeName() + " is found");
	 
	          // Is the input an array<struct<>>
	          // Get the object inspector for the list(array) elements; this should be a StructObjectInspector
	          // Then check that the struct has the correct fields.
	          // Also, store the ObjectInspectors for use later in the evaluate() method
	          loi = ((ListObjectInspector)arguments[0]);
	          soi = ((StructObjectInspector)loi.getListElementObjectInspector());
	 
	          // Are there the correct number of fields?
	          if( soi.getAllStructFieldRefs().size() != 3 )
	               throw new UDFArgumentTypeException(0,"Incorrect number of fields in the struct. "
	                                                     + "The single argument to AddExternalIdToPurchaseDetails should be "
	                                                     + "Array<Struct>"
	                                                     + " but " + arguments[0].getTypeName() + " is found");
	 
	          // Are the fields the ones we want?
	          StructField target = soi.getStructFieldRef("target");
	          StructField quantity = soi.getStructFieldRef("quantity");
	          StructField price = soi.getStructFieldRef("price");
	 
	          if( target==null )
	               throw new UDFArgumentTypeException(0,"No \"target\" field in input structure "+arguments[0].getTypeName());
	          if( quantity==null )
	               throw new UDFArgumentTypeException(0,"No \"quantity\" field in input structure "+arguments[0].getTypeName());
	          if( price==null )
	               throw new UDFArgumentTypeException(0,"No \"price\" field in input structure "+arguments[0].getTypeName());
	 
	          // Are they of the correct types? (primitives WritableLong, WritableInt, WritableFloat)
	          // We store these Object Inspectors for use in the evaluate() method.
	          toi = target.getFieldObjectInspector();
	          qoi = quantity.getFieldObjectInspector();
	          poi = price.getFieldObjectInspector();
	 
	          // First, are they primitives?
	          if(toi.getCategory() != ObjectInspector.Category.PRIMITIVE )
	               throw new UDFArgumentTypeException(0,"Is input primitive? target field must be a bigint; found "+toi.getTypeName());
	          if(qoi.getCategory() != ObjectInspector.Category.PRIMITIVE )
	               throw new UDFArgumentTypeException(0,"Is input primitive? quantity field must be an int; found "+toi.getTypeName());
	          if(poi.getCategory() != ObjectInspector.Category.PRIMITIVE )
	               throw new UDFArgumentTypeException(0,"Is input primitive? price field must be a float; found "+toi.getTypeName());
	 
	          // Second, are they the correct type of primitive?
	          if( ((PrimitiveObjectInspector)toi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG )
	               throw new UDFArgumentTypeException(0,"Is input correct primitive? target field must be a bigint; found "+toi.getTypeName());
	          if( ((PrimitiveObjectInspector)qoi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT )
	               throw new UDFArgumentTypeException(0,"Is input correct primitive? target field must be an int; found "+toi.getTypeName());
	          if( ((PrimitiveObjectInspector)poi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.FLOAT )
	               throw new UDFArgumentTypeException(0,"Is input correct primitive? price field must be a float; found "+toi.getTypeName());
	 
	          // If we get to here, the input is an array<struct>
	 
	          // HOW TO RETURN THE OUTPUT?
	          // A struct<> is stored as an Object[], with the elements being ,,...
	          // See GenericUDFNamedStruct
	          // The object inspector that we set up below and return at the end of initialize() takes care of the names,
	          // so the Object[] only holds the values.
	          // A java ArrayList is converted to a hive array<>, so the output is an ArrayList
	          ret = new ArrayList();
	 
	          // Now set up and return the object inspector for the output of the UDF
	 
	          // Define the field names for the struct<> and their types
	          ArrayList structFieldNames = new ArrayList();
	          ArrayList structFieldObjectInspectors = new ArrayList();
	 
	          structFieldNames.add("diu");
	          structFieldNames.add("quantity");
	          structFieldNames.add("price");
	          structFieldNames.add("externalId");
	 
	          // To get instances of PrimitiveObjectInspector, we use the PrimitiveObjectInspectorFactory
	          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableLongObjectInspector );
	          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableIntObjectInspector );
	          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableFloatObjectInspector );
	          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );
	 
	          // Set up the object inspector for the struct<> for the output
	          StructObjectInspector si2;
	          si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	 
	          // Set up the list object inspector for the output, and return it
	          ListObjectInspector li2;
	          li2 = ObjectInspectorFactory.getStandardListObjectInspector( si2 );
	          return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

	}
*/
}
