package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.autonavi.parse.page.SplitPageLog;
import com.autonavi.parse.page.bodyBean;
import com.autonavi.parse.page.pkgBean;

public class DecryptClientLogLine extends GenericUDF {

	
	private static SplitPageLog decryptProxy = new SplitPageLog();
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		
		// Should be exactly one argument
        if( arguments.length!=1 )
        return null;


        String line = arguments[0].get().toString();
	    if (line == null) {
			System.out.println("find null input");
	    	return null;
	    }
	    
	    try {
			decryptProxy.parse(SplitPageLog.hexStringToBytes(line));
			System.out.println("finish decrypt one line");
			List<Object> ret = parseDecryptLog(decryptProxy.getDecryptLog());
			for(Object o : ret){
				System.out.println(o.toString());
			}
			return ret;
		} catch (Exception e1) {
			// TODO Auto-generated s block
			e1.printStackTrace();
			return null;
		}
	    
	  }

	private List<Object> parseDecryptLog(pkgBean pkg){
		List<Object> ret = new ArrayList<Object>();

		ret.add(pkg.getImei());

		List<List<Object>> entries = new ArrayList<List<Object>>();
		bodyBean tmpbody = null;
		for (int i = 0; i < pkg.getLt().size(); i++) {
			tmpbody=pkg.getLt().get(i);
			List<Object> entry = new ArrayList<Object>();
			entry.add(tmpbody.getSessionid());
			entry.add(tmpbody.getStepid());
			//entry.add(tmpbody.getAid());
			entry.add(tmpbody.getSource());
			entry.add(tmpbody.getService());
			entry.add(tmpbody.getPage());
			entry.add(tmpbody.getButton());
			entry.add(tmpbody.getAction());
			entry.add(Long.toString(tmpbody.getActtime()));
			//entry.add(tmpbody.getSessionid());
			entry.add(tmpbody.getX());
			entry.add(tmpbody.getY());
			entry.add(Utilities.parseJsonStr2Map(tmpbody.getPara()));
			//Map<String, String> position = new HashMap<String, String>();
			//position.put("x", tmpbody.getX());
			//position.put("y", tmpbody.getY());
			entries.add(entry);
		}
		ret.add(entries);

		Map<String, String> cellinfo = new HashMap<String,String>();
		cellinfo.put("model", pkg.getModel());
		cellinfo.put("device", pkg.getDevice());
		cellinfo.put("manufacture", pkg.getManufacture());
		ret.add(cellinfo);

		Map<String, String> others = new HashMap<String, String>();
		others.put("version", pkg.getVer());
		others.put("protocal",pkg.getProtocol_version());
		others.put("diu2", pkg.getDiu2());
		others.put("diu3", pkg.getDiu3());
		others.put("dic", pkg.getDic());
		ret.add(others);
	
		ret.add(Utilities.timestamp2string("yyyyMMdd", pkg.getSessionId() + 129381120000l));
		return ret;
	}
	
	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return "decrypt client log";
	}
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		 if( arguments.length != 1 )
	          throw new UDFArgumentLengthException("DecryptClientLog accepts exactly one argument.");
	 
		 
		      PrimitiveObjectInspector inputOI = ((PrimitiveObjectInspector)arguments[0]);
	          // Is the input an array<>
	          if( inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)
	          throw new UDFArgumentTypeException(0,"The single argument to DecryptClientLog should be "
	          + "String " + "but " + arguments[0].getTypeName() + " is found");
	 
	          ArrayList<String> outputStructFieldNames = new ArrayList<String>();
	          ArrayList<ObjectInspector> outputStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

	          outputStructFieldNames.add("diu");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );


	          /*
	          outputStructFieldNames.add("ver");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          outputStructFieldNames.add("proc_ver");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          outputStructFieldNames.add("diu2");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          outputStructFieldNames.add("diu3");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );
	          
	          outputStructFieldNames.add("dic");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );


	          
	          outputStructFieldNames.add("cellphone");
	          outputStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));
	        						
			*/
	          ArrayList<String> entryStructFieldNames = new ArrayList<String>();
	          ArrayList<ObjectInspector> entryStructFieldObjectInspectors = new ArrayList<ObjectInspector>();
	 
	          outputStructFieldNames.add("sessionid");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaLongObjectInspector );

	          entryStructFieldNames.add("stepid");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaLongObjectInspector );

	          /*
	          entryStructFieldNames.add("aid");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );
	          */

	          entryStructFieldNames.add("source");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("service");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("page");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("button");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("action");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("acttime");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          entryStructFieldNames.add("x");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("y");
	          entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          entryStructFieldNames.add("paras");
	          //entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );
	          entryStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));


	          StructObjectInspector soi =  ObjectInspectorFactory.getStandardStructObjectInspector(
	        		  									entryStructFieldNames, entryStructFieldObjectInspectors);
	          ListObjectInspector loi = ObjectInspectorFactory.getStandardListObjectInspector(soi);
	          
	          outputStructFieldNames.add("clicks");
	          outputStructFieldObjectInspectors.add( loi );
	         
	          outputStructFieldNames.add("cellphone");
	          outputStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));

	          outputStructFieldNames.add("others");
	          outputStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));

	          outputStructFieldNames.add("dt");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          return ObjectInspectorFactory.getStandardStructObjectInspector(
	        		  outputStructFieldNames,
	        		  outputStructFieldObjectInspectors);
	}
	
}
