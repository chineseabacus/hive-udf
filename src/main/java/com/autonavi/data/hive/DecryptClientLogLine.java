package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.autonavi.parse.page.SplitPageLog;
import com.autonavi.parse.page.bodyBean;
import com.autonavi.parse.page.pkgBean;

public class DecryptClientLogLine extends GenericUDTF {

	private final static long epochstep = 1293811200l * 1000;
	
	private static SplitPageLog decryptProxy = new SplitPageLog();
	public void process(Object[] arguments) throws HiveException {
		
		String line = arguments[0].toString();
	    try {
			decryptProxy.parse(SplitPageLog.hexStringToBytes(line));
			System.out.println("finish decrypt one line");
			
			pkgBean pkg = decryptProxy.getDecryptLog();
			flattenDecryptLog(pkg);
		} catch (Exception e1) {
			// TODO Auto-generated s block
			e1.printStackTrace();
		}
	    
	  }
	
	/**
	 * 过滤掉日志收集请求 http://log.amap.com/collector/client/behavior， stepid = 0
	 * 关联客户端点击与网络事件，修正客户端点击正确的stepid
	 * @param beans
	 */
	
	private void processClick(List<bodyBean> beans){

		bodyBean tmpbody = null;
		for (int i = 0; i < beans.size(); i++) {
			List<Object> entry = new ArrayList<Object>();
			tmpbody = beans.get(i);
			if(tmpbody.getStepid() == 0) {
			  beans.remove(i); 
			} else if( (tmpbody.getPage() == 2000 || tmpbody.getPage() == 1000 ) && tmpbody.getButton() == 0){ // request event page = 2000|1000 and button = 0
				/*
				Map<String, String> paraMap = Utilities.parseJsonStr2Map(tmpbody.getPara());
				if(paraMap.get("url").contains("http://s.amap.com/ws/mapapi/poi/tips")){  // sug request 
				*/
				if(tmpbody.getPara().contains("\\/ws\\/mapapi\\/poi\\/"))	
				if(tmpbody.getPara().contains("http:\\/\\/log.amap.com\\/collector\\/client\\/behavior")){  // client log report request
					
				} 
			}
		}
	}

	private void flattenDecryptLog(pkgBean pkg) throws HiveException{

		if(pkg.getVer().substring(4).compareTo("060200") < 0 ) return;
		
		processClick(pkg.getLt());
		bodyBean tmpbody = null;
		for (int i = 0; i < pkg.getLt().size(); i++) {
			tmpbody=pkg.getLt().get(i);
			List<Object> entry = new ArrayList<Object>();
			entry.add(pkg.getImei());
			entry.add(Long.toString(tmpbody.getSessionid()));
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

			Map<String, String> cellinfo = new HashMap<String,String>();
			cellinfo.put("model", pkg.getModel());
			cellinfo.put("device", pkg.getDevice());
			cellinfo.put("manufacture", pkg.getManufacture());
			entry.add(cellinfo);

			Map<String, String> others = new HashMap<String, String>();
			others.put("version", pkg.getVer());
			others.put("protocal",pkg.getProtocol_version());
			others.put("diu2", pkg.getDiu2());
			others.put("diu3", pkg.getDiu3());
			others.put("dic", pkg.getDic());
			entry.add(others);

			entry.add(Utilities.timestamp2string("yyyyMMdd", tmpbody.getActtime() + epochstep));
			
			forward(entry.toArray());
		}

	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "decrypt client log";
	}
	@Override
	public StructObjectInspector initialize(ObjectInspector[] arguments)
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
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          outputStructFieldNames.add("stepid");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaLongObjectInspector );

	          /*
	          outputStructFieldNames.add("aid");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );
	          */

	          outputStructFieldNames.add("source");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("service");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("page");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("button");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("action");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("acttime");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

	          outputStructFieldNames.add("x");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("y");
	          outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

	          outputStructFieldNames.add("paras");
	          //outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );
	          outputStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	        		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));

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

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}
	
}
