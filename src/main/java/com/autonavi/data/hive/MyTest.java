package com.autonavi.data.hive;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class MyTest extends GenericUDTF {

  private transient ListObjectInspector li;

  public MyTest(){
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
    //There should be one argument that is a array of struct
	  
	System.out.println("inoi class " + ois.getClass());
    if (ois.length!=1){
      throw new UDFArgumentException("UDF tables only one argument");
    }
    if (ois[0].getCategory()!= Category.LIST){
      throw new UDFArgumentException("Top level object must be an array but "
              + "was "+ois[0].getTypeName());
    }
    li = (ListObjectInspector) ois[0];
    ObjectInspector sub=li.getListElementObjectInspector();
    if (sub.getCategory() != Category.STRUCT){
      throw new UDFArgumentException("The sub element must be struct, but was "+sub.getTypeName());
    }
    return (StructObjectInspector) sub;
  }

  @Override
  public void process(Object[] os) throws HiveException {
    for (Object row : new ArrayList<Object>(li.getList(os[0]))) {
    	System.out.println("element class " + row.getClass());
      forward(row);
    }
  }

  @Override
  public void close() throws HiveException {
  }

  @Override
  public String toString() {
    return "inline";
  }
}
