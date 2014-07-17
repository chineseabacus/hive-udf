package com.autonavi.data.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class UnionTable extends AbstractGenericUDAFResolver {
	
	  public UnionTable() {
	  }

	  @Override
	  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
	      throws SemanticException {
	    if (parameters.length != 1) {
	      throw new UDFArgumentTypeException(parameters.length - 1,
	          "Exactly one argument is expected.");
	    }
	    if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
	      throw new UDFArgumentTypeException(0,
	          "Only list type arguments are accepted but "
	          + parameters[0].getTypeName() + " was passed as parameter 1.");
	    }
	    return new GenericUDAFMkCollectionEvaluator();
	  }
	  
	  
	  
      /*
	  public static class GenericUDAFMkCollectionEvaluator extends GenericUDAFEvaluator {

		  public GenericUDAFMkCollectionEvaluator(){
			  System.out.println("start to initialize GenericUDAFMkCollectionEvaluator");
		  }
		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		  private transient ListObjectInspector inputOI;
	      // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of objs)

	      private transient StandardListObjectInspector internalMergeOI;
	  
		
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
			      return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
			      
			    } else {
                   
			      System.out.println("input list level = " + computeListLevel(parameters[0])); 
			      System.out.println("get oi for Mode.PARTIAL2");
			      if(computeListLevel(parameters[0]) == 1){
			        System.out.println("no map aggregation.");
			        inputOI = (ListObjectInspector)  ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
			        return (StandardListObjectInspector) ObjectInspectorFactory
			            .getStandardListObjectInspector(inputOI.getListElementObjectInspector());
			      } 
			      else {
			        internalMergeOI = (StandardListObjectInspector) parameters[0];
			        //inputOI = (ListObjectInspector)  ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getListElementObjectInspector());
			        inputOI = (ListObjectInspector) internalMergeOI.getListElementObjectInspector();
			        //return   ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getListElementObjectInspector());
			        return ObjectInspectorFactory.getStandardListObjectInspector(inputOI.getListElementObjectInspector());
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
           System.out.println("iterater object size: " + inputOI.getListLength(table));
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;

	       //for(Object row : inputOI.getList(p)){
	       //	 System.out.println("subelement of iterater object is " + row.getClass());
	       //  putIntoCollection(row, myagg);
	       // }
	       putIntoCollection(table, myagg);
	     }

	     //mapside
	     @Override
	     public Object terminatePartial(AggregationBuffer agg) throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       List<Object> ret = new ArrayList<Object>(myagg.container.size());
	       ret.addAll(myagg.container);
           System.out.println("in terminatePartial size = " + myagg.container.size());
           for(Object o : myagg.container){
                System.out.println("each  count  is " + ((LazyBinaryArray)o).getListLength()); 
           }
	       return ret;
	     }

	     @Override
	     public void merge(AggregationBuffer agg, Object partial)
	         throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       System.out.println("start to merge");
	       List<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
	       if (partialResult != null) {
	         for(Object i : partialResult) {
	        	 System.out.println("merge element class " + i.getClass());
	        	 for(Object row : inputOI.getList(i)) {
	        		 System.out.println("add element class " + row.getClass());
	        		 putIntoCollection(row, myagg);
	        	 }
	         }
	       }
	     }

	     @Override
	     public Object terminate(AggregationBuffer agg) throws HiveException {
	       MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
	       List<Object> ret = new ArrayList<Object>(myagg.container.size());
	       ret.addAll(myagg.container);
           System.out.println("in terminate size = " + myagg.container.size());
	       return ret;
	     }

	     private void putIntoCollection(Object p, MkArrayAggregationBuffer myagg) {
	       //Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,  this.inputOI.getListElementObjectInspector());
	       myagg.container.add(p);
	     }

	  }
	
    */
}
