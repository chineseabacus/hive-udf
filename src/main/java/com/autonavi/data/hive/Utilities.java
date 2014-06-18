package com.autonavi.data.hive;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class Utilities {
	
	public static Map<String, String> parseJsonStr2Map(String jsonstr) {
		
	    if(jsonstr == null || jsonstr.equals(""))
			return null;
	    JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory); 
	   
	    TypeReference<HashMap<String,String>> typeRef 
	            = new TypeReference<HashMap<String,String>>() {};

	    try {
	    	return mapper.readValue(jsonstr, typeRef); 
	    }catch (Exception e){
	    	e.printStackTrace();
	    }
	    
	    return null;
	}
	
	
	public static String timestamp2string (String format, long millseconds){
		return 	new SimpleDateFormat(format).format(new Timestamp(millseconds));
	}
	
	public static void main(String[] args){
		System.out.println("now " + System.currentTimeMillis() + " => " +  timestamp2string("yyyyMMdd" ,System.currentTimeMillis()));
		System.out.println(timestamp2string("yyyyMMdd", 109016106911l + 1293811200000l));
	}

}
