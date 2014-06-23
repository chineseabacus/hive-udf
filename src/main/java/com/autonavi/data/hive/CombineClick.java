package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;

public class CombineClick extends GenericUDTF {

	ListObjectInspector inloi;
	StructObjectInspector insoi;

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2)
			throw new UDFArgumentLengthException(
					"PoiClick accepts exactly two argument.");

		if (arguments[0].getCategory() != Category.LIST)
			throw new UDFArgumentTypeException(0,
					"The first argument to CombineClick should be array "
							+ "but " + arguments[0].getTypeName() + " is found");
		
		inloi = (ListObjectInspector) arguments[0];

		if (inloi.getListElementObjectInspector().getCategory() != Category.STRUCT)
			throw new UDFArgumentTypeException(0,
					"The element of first argument to CombineClick should be struct "
							+ "but "
							+ inloi.getListElementObjectInspector()
									.getCategory() + " is found ");

		//System.out.println("list oi " + arguments[0].getClass());
		//System.out.println("list element oi " + inloi.getListElementObjectInspector().getClass());
		insoi = (StructObjectInspector) inloi.getListElementObjectInspector();
	    //System.out.println("sturct paras oi " + insoi.getStructFieldRef("paras").getFieldObjectInspector().getClass());

	    List<StructField> fields = (List<StructField>) insoi.getAllStructFieldRefs();
	    
	    //return insoi;
	    
		ArrayList<String> outputStructFieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> outputStructFieldObjectInspectors = new ArrayList<ObjectInspector>();

		for(StructField field : fields){
			outputStructFieldNames.add(field.getFieldName());
			if(field.getFieldName().equals("request")){
				outputStructFieldObjectInspectors.add(ObjectInspectorFactory.getStandardMapObjectInspector(
				((MapObjectInspector)field.getFieldObjectInspector()).getMapKeyObjectInspector(),
				((MapObjectInspector)field.getFieldObjectInspector()).getMapValueObjectInspector()
						));
			}
			outputStructFieldObjectInspectors.add(field.getFieldObjectInspector());
		}
		/*
		outputStructFieldNames.add("sessionid");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		outputStructFieldNames.add("stepid");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

		outputStructFieldNames.add("source");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("service");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("page");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("button");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("action");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("acttime");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		outputStructFieldNames.add("x");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("y");
		outputStructFieldObjectInspectors
				.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

		outputStructFieldNames.add("paras");
		// outputStructFieldObjectInspectors.add(
		// PrimitiveObjectInspectorFactory.javaStringObjectInspector );
		outputStructFieldObjectInspectors
				.add(ObjectInspectorFactory
						.getStandardMapObjectInspector(
								PrimitiveObjectInspectorFactory.javaStringObjectInspector,
								PrimitiveObjectInspectorFactory.javaStringObjectInspector));

		*/
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				outputStructFieldNames, outputStructFieldObjectInspectors);
		
	}

	@Override
	public void process(Object[] args) throws HiveException {

		List<List<Object>> ret = new ArrayList<List<Object>>();

		List<Object> tmprowlist = null;
		Map<Object, Object> tmppara = null;

		List<?> indata = inloi.getList(args[0]);
		String urlpattern = args[1].toString();
		ListIterator<?> iter = indata.listIterator(indata.size());

		// Iterate in reverse.
		while (iter.hasPrevious()) {
			// sessionid, stepid, source ,service ,page, button, action, acttime, x, y, para
			Object row = iter.previous();

			List<Object> rowlist = insoi.getStructFieldsDataAsList(row);
			//System.out.println("stepid " + rowlist.get(1));
			//System.out.println("paras " + rowlist.get(10));
			
			//for(Object o : rowlist){
			//	if(o != null)
			//	System.out.println(o.getClass());
			//}

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
			
			// Integer page =
			// PrimitiveObjectInspectorFactory.javaIntObjectInspector.getPrimitiveJavaObject(
			
			/*
			StructField pagefield = insoi.getStructFieldRef("page");
			StructField buttonfield = insoi.getStructFieldRef("button");
			Integer page = (Integer) ((IntObjectInspector) insoi
					.getStructFieldRef("page").getFieldObjectInspector())
					.getPrimitiveJavaObject(insoi.getStructFieldData(row, pagefield));
			Integer button = (Integer)((IntObjectInspector) insoi
					.getStructFieldRef("button").getFieldObjectInspector())
					.getPrimitiveJavaObject( insoi.getStructFieldData(row,buttonfield));
			*/

			//System.out.println("page = " + page + " button = " + button);


			LazyMapObjectInspector ins = ((LazyMapObjectInspector)insoi.getStructFieldRef("request").getFieldObjectInspector());
			LazyStringObjectInspector keyoi = (LazyStringObjectInspector)ins.getMapKeyObjectInspector();
			LazyStringObjectInspector valoi = (LazyStringObjectInspector)ins.getMapValueObjectInspector();

			
			if( page != null && button != null && ( page.toString().equals("2000") || page.toString().equals("1000") ) && button.toString().equals("0")) { //request

			
				/*
				ByteArrayRef urlkeyRef = new ByteArrayRef();
				urlkeyRef.setData(new Text("page").getBytes());
				LazyString urlkey = new LazyString(keyoi);
				urlkey.init(urlkeyRef, 0, urlkeyRef.getData().length);

				System.out.println("find why " + pagekey.getClass());
				System.out.println("find why " + ((LazyString)pagekey).equals(urlkey));
				System.out.println("find why " + (pagekey).equals(urlkey));
				System.out.println("find why " + ((LazyString)pagekey).getWritableObject().getClass());
				System.out.println("find why " + urlkey.getWritableObject().equals(((LazyString)pagekey).getWritableObject()));
				System.out.println("find why " + (urlkey instanceof LazyPrimitive<?, ?>));
				
				*/
			
				/*
				System.out.println("para class " + para.getClass());
				System.out.println("map data" + para);
				
			
				System.out.println("start judge " + (urlkey instanceof LazyPrimitive<?, ?>));
				for(Object key : para.keySet()){
					System.out.println(key.toString() + "\t bytearray" +
					Arrays.toString (((LazyString)key).getWritableObject().copyBytes()) + "\thashcode" + key.hashCode());
					if(key.toString().equals("url")){
							System.out.println(key.getClass());
							System.out.println(key.equals(urlkey));
							System.out.println(((LazyString)key).equals(urlkey));
							System.out.println(((LazyString)key).getWritableObject().equals(urlkey.getWritableObject()));
			
							Object mydata = urlkey.getWritableObject();
							Object theirdata = ((LazyString)key).getWritableObject();
							System.out.println("mydata " + mydata.getClass() + " theirdata " + theirdata.getClass());
							System.out.println(((LazyString)key).getWritableObject ().hashCode());
							System.out.println(urlkey.getWritableObject().hashCode());
			
					}
				}
				*/
				
				//System.out.println("origin" + origin.getClass());
				//System.out.println("para" + para.getClass());
				//System.out.println("origin contain key " + origin.containsKey(urlkey.getObject()));
				/* no use
				Map<Object, Object> test = new HashMap<Object, Object>();
				for(Map.Entry<Object, Object> entry : para.entrySet()){
					
					test.put(entry.getKey(), entry.getValue());
				}
				System.out.println("in test " + test.containsKey(urlkey));
				*/

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

				/*
				for(Object key : para.keySet()){
					if(key.toString().equals("url")){
						//System.out.println( "key == urlkey " + key.equals(urlkey));
						//System.out.println( key.getClass() + "\t " + urlkey.getClass());
						//System.out.println( key.hashCode() + "\t " + urlkey.hashCode());
						//System.out.println( key+ "\t " + urlkey);
						if(para.get(key).toString().contains(urlpattern)) {
								tmprowlist = rowlist;
								tmppara = para;
						}
					}
				}
				*/
				
			
			}
			else {//page-button click
				if(tmppara == null) {// directly add no network request click
					rowlist.set(6, para);
					ret.add(rowlist);
				}
				else{ 
					//insoi.getStructFieldRef("page") 
					//insoi.getAllStructFieldRefs().indexOf).getStructFieldRef("page").getFieldObjec4tInspector().
					if(para != null) {
						//tmppara.put(pagekey, para.get(pagekey));
						//tmppara.put(buttonkey, para.get(buttonkey));
						tmppara.putAll(para);
						System.out.println(tmppara.get(pagekey));
						//System.out.println("tmppara " + tmppara.getClass() + ",para " + (para == null ? null : para.getClass())); 
					}
					else {
						System.out.println("find one null para, which should'n be null, and sessionid =  " +
								rowlist.get(0) + " stepid = " + rowlist.get(1));
					}
					tmprowlist.set(6, tmppara);
					ret.add(tmprowlist);
					tmppara = null;
					tmprowlist = null;
				}
			}
			 
		}

		ListIterator<List<Object>> listiter = ret.listIterator(ret.size());
		while (listiter.hasPrevious()) {
			List<Object> one = listiter.previous();
			forward(one.toArray());
		}
	}

	@Override
	public String toString() {
		return "combine click by url pattern";
	}

}
