package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.autonavi.parse.page.SplitPageLog;
import com.autonavi.parse.page.bodyBean;
import com.autonavi.parse.page.pkgBean;

public class SimpleDecryptClientLog extends GenericUDTF {

	private final static long epochstep = 1293811200l * 1000;
	private static SplitPageLog decryptProxy = new SplitPageLog();

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		
		if( args.length != 1 || args[0].getCategory() != Category.PRIMITIVE )
			throw new UDFArgumentException("SimpleDecryptClientLog only accept one string argument");
		
		List<String> outputFieldNames = new ArrayList<String>();
		List<ObjectInspector> outputFieldOIs = new ArrayList<ObjectInspector>();
		
		outputFieldNames.add("uid");
		outputFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		outputFieldNames.add("sessionid");
		outputFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		/*
		ArrayList<String> entryStructFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> entryStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

        entryStructFieldNames.add("stepid");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("page");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("button");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("source");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("service");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("action");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("acttime");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("x");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("y");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        entryStructFieldNames.add("paras");
        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );
        */
        /*
        entryStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
      		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
      		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));
      		  								*/

        outputFieldNames.add("clicks");
        outputFieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        outputFieldNames.add("others");
        outputFieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

        /*
        outputFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
       	  ObjectInspectorFactory.getStandardStructObjectInspector(entryStructFieldNames,
        				  												  entryStructFieldObjectInspectors)));
       */

        return ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames,outputFieldOIs);
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(Object[] arguments) throws HiveException {
		
		String line = arguments[0].toString();
	    try {
			decryptProxy.parse(SplitPageLog.hexStringToBytes(line));
			//System.out.println("finish decrypt one line");
			
			pkgBean pkg = decryptProxy.getDecryptLog();
			splitLogBySession(pkg);
		} catch (Exception e1) {
			// TODO Auto-generated s block
			e1.printStackTrace();
		}
	}

	private void splitLogBySession(pkgBean pkg) throws HiveException {
		
		if(pkg.getVer().substring(4).compareTo("060200") < 0 ) {
			System.out.println("filter one low version " + pkg.getImei());
			return;
		}

		StringBuffer userinfo = new StringBuffer();
		userinfo.append(pkg.getVer()).append(":");
		userinfo.append(pkg.getProtocol_version()).append(":");
		userinfo.append(pkg.getDiu2()).append(":");
		userinfo.append(pkg.getDiu3()).append(":");
		userinfo.append(pkg.getDic()).append(":");
		userinfo.append(pkg.getModel()).append(":");
		userinfo.append(pkg.getDevice()).append(":");
		userinfo.append(pkg.getManufacture());

		//Map< String,  List< List<Object> > > sessionRet = new HashMap<String,  List< List<Object> > >();
		Map< String,  StringBuffer > sessionRet = new HashMap<String,  StringBuffer >();
		String lastSession = null;
		//List< List<Object> > entries = null;
		StringBuffer entries = null;
		List<Object> entry = null;
		
		bodyBean tmpbody = null;
		for (int i = 0; i < pkg.getLt().size(); i++) {
			tmpbody=pkg.getLt().get(i);

			String sessionid = Long.toString(tmpbody.getSessionid());

			if( !sessionid.equals(lastSession)){
				//sessionRet.put(sessionid , new ArrayList< List<Object> >());
				sessionRet.put(sessionid , new StringBuffer());
				entries = sessionRet.get(sessionid);
				lastSession = sessionid ;
			}

			entries.append(tmpbody.getStepid()+ClientLogParseConf.FIELD_SEP);
			entries.append(tmpbody.getPage()+ClientLogParseConf.FIELD_SEP);
			entries.append(tmpbody.getButton()+ClientLogParseConf.FIELD_SEP);
			entries.append(tmpbody.getSource()+ClientLogParseConf.FIELD_SEP);
			entries.append(tmpbody.getService()+ClientLogParseConf.FIELD_SEP);
			entries.append(tmpbody.getAction()+ClientLogParseConf.FIELD_SEP);
			entries.append(Utilities.timestamp2string("HH:mm:ss", tmpbody.getActtime() + epochstep)+ClientLogParseConf.FIELD_SEP);
			entries.append("{\"x\":\"" + tmpbody.getX() + "\"," + "\"y\":\""+ tmpbody.getY() + "\"}" + ClientLogParseConf.FIELD_SEP);
			if(tmpbody.getPara().equals("") || tmpbody.getPara() == null)
				entries.append("{}" + ClientLogParseConf.LINE_SEP);
			else
				entries.append(tmpbody.getPara()+ ClientLogParseConf.LINE_SEP);

			/*
			entries.add(new ArrayList<Object>());
			entry = entries.get(entries.size()-1);


			entry.add(tmpbody.getStepid()+"");
			entry.add(tmpbody.getPage()+"");
			entry.add(tmpbody.getButton()+"");
			entry.add(tmpbody.getSource()+"");
			entry.add(tmpbody.getService()+"");
			entry.add(tmpbody.getAction()+"");
			entry.add(Utilities.timestamp2string("HH:mm:ss", tmpbody.getActtime() + epochstep));
			entry.add(tmpbody.getX()+"");
			entry.add(tmpbody.getY()+"");
			entry.add(tmpbody.getPara()+"");
			//Map<String,String> request = Utilities.parseJsonStr2Map(tmpbody.getPara());
			//entry.add(request);
			 
			 */
		}
		//sessionRet.put(lastSession, entries);

		List<Object> ret = new ArrayList<Object>();
		//for( Map.Entry<String, List< List<Object> >> one : sessionRet.entrySet()) {
		for( Map.Entry<String, StringBuffer> one : sessionRet.entrySet()) {
			ret.add(pkg.getImei());
			ret.add(one.getKey()); //session id
			ret.add(one.getValue().toString()); //click list
			ret.add(userinfo.toString());
			//System.out.println(pkg.getImei() + " , " + one.getKey() + " => " + one.getValue());
			forward(ret.toArray());
			ret.clear();
		}
	}

}
