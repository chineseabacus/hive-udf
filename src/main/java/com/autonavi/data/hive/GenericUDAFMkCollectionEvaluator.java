package com.autonavi.data.hive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;



public class  GenericUDAFMkCollectionEvaluator extends GenericUDAFEvaluator {


		  public GenericUDAFMkCollectionEvaluator(){
			  System.out.println("start to initialize GenericUDAFMkCollectionEvaluator");
		  }
		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		  private transient ListObjectInspector inputOI;
	      // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of objs)

	      private transient StandardListObjectInspector internalMergeOI;

		private StructObjectInspector inputEleOI;
	  
		
		class MkArrayAggregationBuffer implements AggregationBuffer {

		   private Collection<Object> container;

		   public MkArrayAggregationBuffer() {
		        container = new ArrayList<Object>();
		   }
		}
		
		private int computeListLevel(ObjectInspector oi){
			int listlevel = 0;
			while(oi instanceof ListObjectInspector) {
			    	  listlevel ++;
			    	  oi = ((ListObjectInspector)oi).getListElementObjectInspector();
			      }
			return listlevel;
		}
		
		@Override 
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
			      throws HiveException {
			    super.init(m, parameters);
			    // init output object inspectors
			    // The output of a partial aggregation is a list
			    System.out.println("input element oi class" + parameters[0].getClass());

			    if (m == Mode.PARTIAL1) {
			      System.out.println("get oi for Mode.PARTIAL1");
			      inputOI = (ListObjectInspector) parameters[0];
			      
			      System.out.println("child of input element oi class" + inputOI.getListElementObjectInspector().getClass());
			      //return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
			      return ObjectInspectorFactory.getStandardListObjectInspector(
			    		  ObjectInspectorUtils.getStandardObjectInspector(inputOI));
			    } else {
                   
			      System.out.println("input list level = " + computeListLevel(parameters[0])); 
			      System.out.println("get oi for Mode.PARTIAL2");
			      if(computeListLevel(parameters[0]) == 1){
			        System.out.println("no map aggregation.");
			        inputOI = (ListObjectInspector) parameters[0];
			        //return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
			        return ObjectInspectorFactory.getStandardListObjectInspector(
			        ObjectInspectorUtils.getStandardObjectInspector(inputOI));
			      } 
			      else {
			        internalMergeOI = (StandardListObjectInspector) parameters[0];
			        //inputOI = (ListObjectInspector)  ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getListElementObjectInspector());
			        inputOI = (ListObjectInspector) internalMergeOI.getListElementObjectInspector();
			        //return   ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getListElementObjectInspector());
			        //return ObjectInspectorFactory.getStandardListObjectInspector(inputOI.getListElementObjectInspector());
			        return ObjectInspectorFactory.getStandardListObjectInspector(
			    		  ObjectInspectorUtils.getStandardObjectInspector(inputOI.getListElementObjectInspector()));
			       }
			    }
		 }


	     @Override
	     public void reset(AggregationBuffer agg) throws HiveException {
	       ((MkArrayAggregationBuffer) agg).container.clear();
	     }

	     @Override
	     public AggregationBuffer getNewAggregationBuffer() throws HiveException {
	       MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
	       return ret;
	     }

	     //mapside
	     @Override
	     public void iterate(AggregationBuffer agg, Object[] parameters)
	         throws HiveException {
	       assert (parameters.length == 1);
	       Object table = parameters[0];

	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
           //System.out.println("one table in buffer: " + myagg.container.hashCode() + " size: " + inputOI.getListLength(table));
	       putIntoCollection(table, myagg);
	     }

	     //mapside
	     @Override
	     public Object terminatePartial(AggregationBuffer agg) throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       List<Object> ret = new ArrayList<Object>(myagg.container.size());
	       ret.addAll(myagg.container);
	       return ret;
	     }

	     @Override
	     public void merge(AggregationBuffer agg, Object partial)
	         throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       //System.out.println("start to merge");
	       List<Object>  tables = (ArrayList<Object>) internalMergeOI.getList(partial);
	       if ( tables != null) {
	         for(Object table : tables) {
	        	 // System.out.println("merge element class " + table.getClass());
	        	 for(Object row : inputOI.getList(table)) {
	        		// System.out.println("add element class " + row.getClass());
	        		 Object pCopy = ObjectInspectorUtils.copyToStandardObject(row,  this.inputOI.getListElementObjectInspector());
	        		 myagg.container.add(pCopy);
	        	 }
	         }
	       }
	     }

	     @Override
	     public Object terminate(AggregationBuffer agg) throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       List<Object> ret = new ArrayList<Object>(myagg.container.size());
	       ret.addAll(myagg.container);
	       return ret;
	     }

	     private void putIntoCollection(Object p, MkArrayAggregationBuffer myagg) {
	       Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,  this.inputOI);
	       myagg.container.add(pCopy);
	     }

}
