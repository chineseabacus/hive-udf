package com.autonavi.data.hive;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class Utilities {
	
	public static Map<String, String> parseJsonStr2Map(String jsonstr) {
		
	    if(jsonstr == null || jsonstr.equals(""))
            return new HashMap<String, String>();
	    JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory); 
	   
	    TypeReference<HashMap<String,String>> typeRef 
	            = new TypeReference<HashMap<String,String>>() {};

	    try {
	    	return mapper.readValue(jsonstr, typeRef); 
	    }catch (Exception e){
	    	e.printStackTrace();
	    }
	    
	    return  new HashMap<String, String>();
	}
	
	
	public static String timestamp2string (String format, long millseconds){
		return 	new SimpleDateFormat(format).format(new Timestamp(millseconds));
	}
	
	static class MyObj {
		String data;
		public MyObj(String str){
			data = str;
		}
		public String toString() {
			return data;
		}
	}
	public static void main(String[] args){
		
		List<MyObj> testList = new ArrayList<MyObj>();
		MyObj ori = new MyObj("origin");
		testList.add(ori);
		
		ori = new MyObj("changed");
		System.out.println(testList);
		
		
		System.out.println("now " + System.currentTimeMillis() + " => " +  timestamp2string("yyyyMMdd" ,System.currentTimeMillis()));
		System.out.println(timestamp2string("yyyyMMdd", 109016106911l + 1293811200000l));
		
		Integer[] data = {9,11,20,41,42,55,64,75,84,100,120};
		List<Integer> datalist = Arrays.asList(data);
		Integer[] tags = {8,25,40,80};
		List<Integer> tagslist = Arrays.asList(tags);
		
		Utilities util = new Utilities();
		List< List<Integer> > res = util.splitByTags(datalist, tagslist, new Comparator<Integer>() {
												public int compare(Integer a, Integer b) {
													return a > b ? 1 : ((a == b) ? 0 : -1);
												}
											}
						);
		
		System.out.println(res);
		
	}
	
	public <T> List< List<T> >  splitByTags(List<T> data, List<T> tags, Comparator<T> comparator ){
		
		List< List<T> > ret = new ArrayList<List<T>>();
		if(tags.size() <= 1 || data.size() <= 1 ) {
			ret.add(data);
			return ret; 
		}

		ret.add(new ArrayList<T>()); 
		List<T> subret  = ret.get(ret.size() -1 );

		ListIterator< T > dataIter= data.listIterator();
		ListIterator< T > tagIter= tags.listIterator();
		T one = dataIter.next();
		T tag = tagIter.next();
		while(true) {
			if(comparator.compare(one, tag) <= 0 ) { // || ! tagIter.hasNext()) {
					subret.add(one);
					if(dataIter.hasNext()) one = dataIter.next();
					else break;
			}
			else {
				if(tagIter.hasNext())
					tag = tagIter.next();
				else 
					tag = data.get(data.size()-1);
				if(subret.size()!=0){
					ret.add(new ArrayList<T>());
					subret = ret.get(ret.size()-1);
				}
			}
		}
		return ret;
	}

	public <T> List< List<T> >  splitByTags1(List<T> data, List<T> tags, Comparator<T> comparator ){
		List< List<T> > ret = new ArrayList<List<T>>();
		if(tags.size() <= 1 || data.size() <= 1 ) {
			ret.add(data);
			return ret; 
		}

		int index = 0;
		T tag1= tags.get(index);
		T tag2= tags.get(index++);

		
		//if( comparator.compare(data1, tag1) < 0) {
		//	tag2 = tag1; 
		//	tag1 = data1;
	    //}

		ret.add(new ArrayList<T>());
		List<T> subret  = ret.get(ret.size() -1 );

		ListIterator< T > dataIter= data.listIterator();
		while(dataIter.hasNext()) {
			T one = dataIter.next();
			if( comparator.compare(one, tag1) >= 0 && comparator.compare(one,tag2) <= 0) {
					subret.add(one);
			}
			else if ( comparator.compare(one, tag2) > 0 ) {
				tag1 = tag2;
				if(index <= tags.size() - 1){
					tag2 = tags.get(index++);
				}
				ret.add(new ArrayList<T>());
				subret = ret.get(ret.size() -1 );
				subret.add(one);
			}
		}
		
		return ret;

	}

	public static <T> T parseJosnFromFile(String jsonfile)
	{
		   if(jsonfile == null || jsonfile.equals(""))
	            return null ;
		    JsonFactory factory = new JsonFactory(); 
		    ObjectMapper mapper = new ObjectMapper(factory); 
		   
		    TypeReference<T> typeRef 
		            = new TypeReference<T>() {};

		    try {
		    	return mapper.readValue(new File(jsonfile), typeRef); 
		    }catch (Exception e){
		    	e.printStackTrace();
		    }
		    
		    return null;
	}

		public static Map<String,Map<String, Map<String, Map<String, String>>>> loadPageButton(String jsonfile){

		   
		   if(jsonfile == null || jsonfile.equals(""))
	            return null ;
		    JsonFactory factory = new JsonFactory(); 
		    ObjectMapper mapper = new ObjectMapper(factory); 
		   
		    TypeReference<HashMap<String,Map<String, Map<String, Map<String, String>>>>> typeRef 
		            = new TypeReference<HashMap<String,Map<String, Map<String, Map<String, String>>>>>() {};

		    try {
		    	return mapper.readValue(new File(jsonfile), typeRef); 
		    }catch (Exception e){
		    	e.printStackTrace();
		    }
		    
		    return null;
	}
		
}
