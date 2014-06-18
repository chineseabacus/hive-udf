package com.autonavi.data.hive;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String str = "IOSH060100";
        System.out.println(str.substring(4));
        if(str.substring(4).compareTo("060200") < 0 ) 
        	System.out.println("low");
        else
        	System.out.println("high");
        	
    }
}
