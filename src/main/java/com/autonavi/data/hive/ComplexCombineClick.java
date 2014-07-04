package com.autonavi.data.hive;

import java.util.ArrayList;
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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;


/**
 * 依据url过滤掉日志收集请求, 并关联客户端点击与网络事件，修正客户端点击正确的stepid
 */
public class ComplexCombineClick extends GenericUDTF {

	ListObjectInspector inloi;
	ListObjectInspector queryStepidloi;
	StructObjectInspector insoi;

	//using hive add command to add page-button json file to distributed cache named page-button.json,
	//then load it as a complex map
	private final static String pagebuttonfile = "./page-button.json";
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
		
		inloi = (ListObjectInspector) arguments[0];
		queryStepidloi = (ListObjectInspector) arguments[2];
		
		if (inloi.getListElementObjectInspector().getCategory() != Category.STRUCT)
			throw new UDFArgumentTypeException(0,
					"The element of first argument to CombineClick should be struct "
							+ "but "
							+ inloi.getListElementObjectInspector()
									.getCategory() + " is found ");

		insoi = (StructObjectInspector) inloi.getListElementObjectInspector();

	    List<StructField> fields = (List<StructField>) insoi.getAllStructFieldRefs();
	    
	    //return insoi;
	    
		ArrayList<String> outputStructFieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> outputStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

		ArrayList<String> entryStructFieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> entryStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

		for(StructField field : fields){
			entryStructFieldNames.add(field.getFieldName());
			if(field.getFieldName().equals("request")){
				entryStructFieldObjectInspectors.add(ObjectInspectorFactory.getStandardMapObjectInspector(
				((MapObjectInspector)field.getFieldObjectInspector()).getMapKeyObjectInspector(),
				((MapObjectInspector)field.getFieldObjectInspector()).getMapValueObjectInspector()
						));
			}
			entryStructFieldObjectInspectors.add(field.getFieldObjectInspector());
		}
		
		outputStructFieldNames.add("stepid");
		outputStructFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

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

		List<?> inData = inloi.getList(args[0]);
		String urlpattern = args[1].toString();
		List<?> queryStepidData = queryStepidloi.getList(args[2]);

		String version = null;
		if(args[3] != null) version = args[3].toString();
		
		
		ListIterator<?> iter = inData.listIterator(inData.size());
		// Iterate in reverse.
		while (iter.hasPrevious()) {
			// sessionid, stepid, source ,service ,page, button, action, acttime, x, y, para
			Object row = iter.previous();

			List<Object> rowlist = insoi.getStructFieldsDataAsList(row);
			List<StructField> fields = (List<StructField>) insoi.getAllStructFieldRefs();

			for(StructField field : fields){
				insoi.getStructFieldData(row, field);
			}
			
			Map<?, ?> origin = ((MapObjectInspector) insoi
					.getStructFieldRef("request").getFieldObjectInspector())
					.getMap(insoi.getStructFieldData(row, insoi.getStructFieldRef("request")));
			Map<Object, Object> para = (Map<Object, Object>) origin;
			
			Object page = null;
			Object pagekey = null;
			Object button = null;
			Object buttonkey = null;
			String url = null;

			if(para != null){
				for(Object key: para.keySet()){
					if(key.toString().equals("page")){
						page = para.get(key);
						pagekey = key;
					}
					if(key.toString().equals("button")){
						button = para.get(key);
					        buttonkey = key;
					}
					if(key.toString().equals("url"))
						url = para.get(key).toString();
				}
			}
			
			if( page != null && button != null &&
					( page.toString().equals("2000") || page.toString().equals("1000") ) && button.toString().equals("0")) { //request
				//if(para != null && para.get(urlkey).toString().contains(urlpattern))
				if(url != null && url.contains(urlpattern) && !url.contains("/ws/mapapi/poi/tips/")){
								tmprowlist = rowlist;
								tmppara = para;
								tmppara.remove(pagekey);
								tmppara.remove(buttonkey);
				}
				else {
					//System.out.println( url + " not contain " + urlpattern);
					continue;
				}
			}
			else {//page-button click
				if(tmppara == null) {// directly add no network request click
					try{
							para.put(new Text("actname"), new Text(versionPageButton.get(version).get(page.toString()).get(button.toString()).get("explain")));
						}catch (Exception e){
						if(!versionPageButton.containsKey(version))
							para.put(new Text("actname"), new Text("unknow version"));
						else if(!versionPageButton.get(version).containsKey(page.toString()))
							para.put(new Text("actname"), new Text("unknow page in version " + version));
						else if(!versionPageButton.get(version).get(page.toString()).containsKey(button.toString()))
							para.put(new Text("actname"), new Text("unknow button in page " + page + " of version " + version));
						}
					rowlist.set(5, para);
					ret.add(rowlist);
				}
				else{ 
					if(para != null) {
						tmppara.putAll(para);
						try{
							tmppara.put(new Text("actname"), new Text(versionPageButton.get(version).get(page.toString()).get(button.toString()).get("explain")));
						}catch (Exception e){
						if(!versionPageButton.containsKey(version))
							tmppara.put(new Text("actname"), new Text("unknow version"));
						else if(!versionPageButton.get(version).containsKey(page.toString()))
							tmppara.put(new Text("actname"), new Text("unknow page in version " + version));
						else if(!versionPageButton.get(version).get(page.toString()).containsKey(button.toString()))
							tmppara.put(new Text("actname"), new Text("unknow button in page " + page + " of version " + version));
						}
					}
					else {
						System.out.println("find one null para, which should'n be null, and sessionid =  " +
								rowlist.get(0) + " stepid = " + rowlist.get(1));
					}
					tmprowlist.set(5, tmppara);
					ret.add(tmprowlist);
					tmppara = null;
					tmprowlist = null;
				}
			}
			 
		}

		
		
		/*
		 * 利用sp中同一session中的stepid集合划分client日志中的不同query
		 */

		List<List<Object>> finalRet = new ArrayList< List<Object> >();
		
		
		ListIterator<?> stepidIter = queryStepidData.listIterator();
		Integer querystepid = Integer.valueOf(stepidIter.next().toString());

		// sort all session click descend
		Collections.sort(ret, new Comparator<List<Object>>(){
			public int compare(List<Object> arg0, List<Object> arg1) {
				// TODO Auto-generated method stub
				return Long.valueOf(arg0.get(0).toString()) > Long.valueOf(arg1.get(0).toString()) ? -1 : 1;
			}});
		

		if(queryStepidData.size() <= 1 || ret.size() <= 1) {
			forward(new Object[]{querystepid, ret});
			return;
		}

		// sort all session click descend
		//Collections.sort(ret, new Comparator<List<Object>>(){
		//	public int compare(List<Object> arg0, List<Object> arg1) {
		//		// TODO Auto-generated method stub
		//		return Long.valueOf(arg0.get(0).toString()) > Long.valueOf(arg1.get(0).toString()) ? -1 : 1;
		//	}});
		
		ListIterator<List<Object>> dataIter = ret.listIterator(ret.size());
		List<Object> data = dataIter.previous();
		Integer clickstepid = Integer.valueOf(data.get(0).toString());
		
		
		while(dataIter.hasPrevious()){
			if( clickstepid.compareTo(querystepid) <= 0 ){
				finalRet.add(data);
				data = dataIter.previous();
				//if(dataIter.hasPrevious()) data = dataIter.previous();
				//else break;
			}
			else{
				if(finalRet.size()!=0){
					System.out.println("forward " + finalRet + " for querystepid " + querystepid);
					forward(new Object[]{querystepid,finalRet});
					finalRet.clear();
				}
				if( stepidIter.hasNext())
					querystepid = Integer.valueOf(stepidIter.next().toString());
				else 
					querystepid = Integer.valueOf(ret.get(0).get(0).toString()); //max stepid in ret assign to querystepid
			}
		}
		forward(new Object[]{querystepid,finalRet});
	}

	@Override
	public String toString() {
		return "combine click by url pattern";
	}
	

}
