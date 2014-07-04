package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;


/**
 * 依据url过滤掉日志收集请求, 并关联客户端点击与网络事件，
 */
public class SimpleCombineClick extends GenericUDTF {

	private static  ListObjectInspector inListOI;
	private static 	ListObjectInspector queryStepidloi;
	

	//using hive add command to add page-button json file to distributed cache named page-button.json,
	//then load it as a complex map
	private final static String pagebuttonfile = "./page-button.json";

	//stepid, page, button, source, service, action, act_time, position, paras , (act_name)
	private final static int stepid_index = 0;
	private final static int page_index = 1;
	private final static int button_index = 2;
	private final static int new_stepid_index = 10; //just to store new_stepid for join with sp group stepids
	

	private static Map<String, Map<String, Map<String, Map<String, String>>>> versionPageButton = Utilities.loadPageButton(pagebuttonfile) ;

	@Override
	public void close() throws HiveException {
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length < 4)
			throw new UDFArgumentLengthException(
					"PoiClick accepts at least four argument. <list of client click>, <focus url>, <query stepids> , <version>");

		if (arguments[0].getCategory() != Category.LIST)
			throw new UDFArgumentTypeException(0,
					"The first argument to CombineClick should be array "
							+ "but " + arguments[0].getTypeName() + " is found");
		


		//inListOI = ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(((ListObjectInspector)arguments[0]).getListElementObjectInspector()));
		inListOI = (ListObjectInspector)arguments[0];
		queryStepidloi = (ListObjectInspector) arguments[2];
		
		System.out.println("element " + inListOI.getListElementObjectInspector());

		if (inListOI.getListElementObjectInspector().getCategory() != Category.PRIMITIVE)
			throw new UDFArgumentTypeException(0,
					"The element of first argument to CombineClick should be struct "
							+ "but "
							+ inListOI.getListElementObjectInspector()
									.getCategory() + " is found ");


	    
		ArrayList<String> outputStructFieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> outputStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

		ArrayList<String> entryStructFieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> entryStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

		outputStructFieldNames.add("querystepid");
	        outputStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

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

	        entryStructFieldNames.add("act_time");
	        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );


	        entryStructFieldNames.add("position");
	        entryStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	          		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	          		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));
	        entryStructFieldNames.add("paras");
	        entryStructFieldObjectInspectors.add( ObjectInspectorFactory.getStandardMapObjectInspector(
	      		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
	      		  								PrimitiveObjectInspectorFactory.javaStringObjectInspector));
		
	        entryStructFieldNames.add("act_name");
	        entryStructFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );

		outputStructFieldNames.add("queryclicks");
		outputStructFieldObjectInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(
				ObjectInspectorFactory.getStandardStructObjectInspector(entryStructFieldNames, entryStructFieldObjectInspectors)
				));

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				outputStructFieldNames, outputStructFieldObjectInspectors);
		
	}

	@Override
	public void process(Object[] args) throws HiveException {

		List<List<Object>> ret = new ArrayList<List<Object>>();

		List<Object> tmprowlist = null;
		Map<Object, Object> tmppara = null;

		List<Text> inData = (List<Text>)inListOI.getList(args[0]);
		if(inData == null || inData.size() == 0) return;
		String urlpattern = args[1].toString();
		List<?> queryStepidData = queryStepidloi.getList(args[2]); //why queryStepid is List<Text>

		String version = null;
		if(args[3] != null) version = args[3].toString();
		
		ListIterator<Text> iter = inData.listIterator();

		List<String> rows = new ArrayList<String>();
		
		// Iterate in reverse.
		while (iter.hasNext()) {
			// sessionid, stepid, source ,service ,page, button, action, acttime, x, y, paras
			String tmp = iter.next().toString();
		    if(tmp.equals("") || tmp == null)
		    	continue;
			rows.addAll(Arrays.asList(tmp.split(ClientLogParseConf.LINE_SEP)));
		}

		// sort all session click ascend
		Collections.sort(rows, new Comparator<String>(){
			public int compare(String arg0, String arg1) {
				// TODO Auto-generated method stub
				return Long.valueOf(arg0.split(ClientLogParseConf.FIELD_SEP)[0]) > Long.valueOf(arg1.split(ClientLogParseConf.FIELD_SEP)[0]) ? 1 : -1;
			}});
		
		ListIterator<String> listiter = rows.listIterator(rows.size());
		while(listiter.hasPrevious()){
			    String row = listiter.previous();
				//stepid, page, button, source, service, action, act_time, position, paras 
				List<Object> rowlist = new ArrayList<Object>(Arrays.asList((Object[])row.split(ClientLogParseConf.FIELD_SEP)));
				
				String page = rowlist.get(page_index).toString();
				String button = rowlist.get(button_index).toString();
				Map<String, String> paras = Utilities.parseJsonStr2Map(rowlist.get(rowlist.size()-1).toString());
				rowlist.set(rowlist.size()-1, paras);
				Map<String, String> position = Utilities.parseJsonStr2Map(rowlist.get(rowlist.size()-2).toString());
				rowlist.set(rowlist.size()-2, position);
				
				String url = paras.get("url");

			if( page != null && button != null &&
					( page.equals("2000") || page.equals("1000") ) && button.equals("0")) { //request
				//if(para != null && para.get(urlkey).toString().contains(urlpattern))
				if(url != null && url.contains(urlpattern) && !url.contains("/ws/mapapi/poi/tips")){
								tmprowlist = rowlist;
				}
				else {
					continue;
				}
			}
			else {//page-button click
				if(tmprowlist == null) {// directly add non network request click
					try{
						rowlist.add(versionPageButton.get(version).get(page).get(button).get("explain"));
						}catch (Exception e){
						if(!versionPageButton.containsKey(version))
							rowlist.add( "unknow version");
						else if(!versionPageButton.get(version).containsKey(page.toString()))
							rowlist.add( "unknow page in version " + version);
						else if(!versionPageButton.get(version).get(page.toString()).containsKey(button.toString()))
							rowlist.add( "unknow button in page " + page + " of version " + version);
						}
					//rowlist.set(new_stepid_index,rowlist.get(stepid_index));
                    rowlist.add(rowlist.get(stepid_index));
					ret.add(rowlist);
				}
				else{// try to combine network request click record with responding network request record
						tmprowlist.set(page_index, rowlist.get(page_index));
						tmprowlist.set(button_index, rowlist.get(button_index));
						try{
							tmprowlist.add(versionPageButton.get(version).get(page).get(button).get("explain"));
						}catch (Exception e){
						if(!versionPageButton.containsKey(version))
							tmprowlist.add("unknow version");
						else if(!versionPageButton.get(version).containsKey(page.toString()))
							tmprowlist.add("unknow page in version " + version);
						else if(!versionPageButton.get(version).get(page.toString()).containsKey(button.toString()))
							tmprowlist.add("unknow button in page " + page + " of version " + version);
						}

						//tmprowlist.set(new_stepid_index,tmprowlist.get(stepid_index));
                        tmprowlist.add(tmprowlist.get(stepid_index));
						tmprowlist.set(stepid_index, rowlist.get(stepid_index));
						ret.add(tmprowlist);
						tmprowlist = null;
				}
					
				}
		}
	
		
		/*
		 * 利用sp中同一session中的stepid集合划分client日志中的不同query
		 */

		if(ret.size() == 0) return;

		List<List<Object>> finalRet = new ArrayList< List<Object> >();
		

		Collections.sort(queryStepidData, new Comparator<Object>(){
			public int compare(Object arg0, Object arg1) {
				// TODO Auto-generated method stub
				return Long.valueOf(arg0.toString()) > Long.valueOf(arg1.toString()) ? 1 : -1;
			}});
		

		ListIterator<?> stepidIter = queryStepidData.listIterator();
		Integer querystepid1 = Integer.valueOf(stepidIter.next().toString());

		//System.out.println("querystepids are " + queryStepidData);
		Integer querystepid2 ;
		 if(queryStepidData.size() <= 1 ) {
		     querystepid2 = Integer.valueOf(ret.get(0).get(new_stepid_index).toString()); //max clickstepid in ret assign to querystepid
		 }
		 else
			 querystepid2 = Integer.valueOf(stepidIter.next().toString());

		
		ListIterator<List<Object>> dataIter = ret.listIterator(ret.size());
		List<Object> data = null;
		Integer clickstepid = -1; 
		
		
		while(dataIter.hasPrevious() && !querystepid1.equals(querystepid2)) {
			System.out.println("start to compare " + clickstepid + " with " + querystepid1 + " and " + querystepid2);
			if( clickstepid.compareTo(querystepid1) < 0 ){
				data = dataIter.previous();
				clickstepid = Integer.valueOf(data.get(new_stepid_index).toString());
			}
			else{
				if(clickstepid.compareTo(querystepid2) < 0 ){
					finalRet.add(data.subList(0, data.size()-1));
					data = dataIter.previous();
					clickstepid = Integer.valueOf(data.get(new_stepid_index).toString());
				}
				else {
					//System.out.println(clickstepid + " large than " + querystepid2);
					if(finalRet.size() != 0 )
					{	
						//System.out.println("forward querystepid " + querystepid1 + " with " + finalRet);
						forward(new Object[]{querystepid1, finalRet});
						finalRet.clear();
					}
					else if( stepidIter.hasNext()) {
						querystepid1 = querystepid2;
						querystepid2 = Integer.valueOf(stepidIter.next().toString());
					}
					else {
						querystepid1 = querystepid2;
					    querystepid2 = Integer.valueOf(ret.get(0).get(new_stepid_index).toString()); //max clickstepid in ret assign to querystepid
					}
				}
			}
		}
		if(querystepid1 == querystepid2 ) {
			while(dataIter.hasPrevious()){
                data = dataIter.previous();
				finalRet.add(data.subList(0,data.size()-1));
			}
		}
		//System.out.println("forward querystepid " + querystepid1 + " with " + finalRet);
		forward(new Object[]{querystepid1,finalRet});
	}

	@Override
	public String toString() {
		return "combine click by url pattern";
	}
	

}
