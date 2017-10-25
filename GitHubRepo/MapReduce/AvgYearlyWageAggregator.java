package H1BPetitionData;

import java.io.IOException;

import java.util.Collections;


import java.util.Map;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvgYearlyWageAggregator implements Tool {
/*Find the average Prevailing Wage for each Job for each Year 
 * (take part time and full time separate).
	Arrange the output in descending order [Certified and Certified Withdrawn.]*/
	
 /*(1 s_no int,2 case_status string,3 employer_name string,4 soc_name string,5 job_title string,
  * 6 full_time_position string,7 prevailing_wage int,8 year string,9 worksite string,
  *  10 longitude double,11 latitude double)
  *  
 */
	   //Map class
    public static Map<String, Double> jobWageMap = new TreeMap<String, Double>(Collections.reverseOrder());
    public static Map<String, Double> jobCount = new TreeMap<String, Double>(Collections.reverseOrder());				
    
    public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {

	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().trim().split("\\t",0);
	            String year=str[7].trim();
	            String wage = str[6].trim();
	            String job = str[4].trim();
	            String full_time_position = str[5].trim();
	            String case_status = str[1].trim();
	            if(/*year.equals("2011") && */full_time_position.equals("N") && ((case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))))
	            	context.write(new Text(job), new Text( wage + '\t' + year));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   
	   //Reducer class
		
	   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
	      
	      String  nYear;
	      private Double totalWage = new Double(0.0);
		    String job = "";
		    private  Map<Double,String> output2011Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private  Map<Double,String> output2012Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private  Map<Double,String> output2013Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private  Map<Double,String> output2014Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private  Map<Double,String> output2015Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private  Map<Double,String> output2016Map = new TreeMap<Double,String>(Collections.reverseOrder());
		    private int jobcount = 0;
		    public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {

	         totalWage = 0.0;
	         job = key.toString().trim();
	         jobcount = 0;
	        for (Text val : values)
	         {
		    	 String[] str = val.toString().trim().split("\\t",0);
		         nYear = str[1].trim();
		         
		         Double wage = Double.parseDouble(str[0].trim());
		         totalWage += wage;
		         
		         jobcount++;
		        	 
	         }
	        Double avg = totalWage/jobcount;
	        Math.round(avg);
	         if(nYear.equals("2011"))
	         {
	        	 if(output2011Map.containsKey(avg))
	        	 {
	        		 String value = output2011Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2011Map.put(avg, value);
	        	 }
	        	 else
		     		output2011Map.put(avg, job);
	         }
	         else if(nYear.equals("2012")){
	        	 if(output2012Map.containsKey(avg))
	        	 {
	        		 String value = output2012Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2012Map.put(avg, value);
	        	 }
	        	 else
		     		output2012Map.put(avg, job);

        	}
        	else if(nYear.equals("2013")){
	        	 if(output2013Map.containsKey(avg))
	        	 {
	        		 String value = output2013Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2013Map.put(avg, value);
	        	 }
	        	 else
		     		output2013Map.put(avg, job);

        	}
        	else if(nYear.equals("2014")){
	        	 if(output2014Map.containsKey(avg))
	        	 {
	        		 String value = output2014Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2014Map.put(avg, value);
	        	 }
	        	 else
		     		output2014Map.put(avg, job);

        	}
        	else if(nYear.equals("2015")){
	        	 if(output2015Map.containsKey(avg))
	        	 {
	        		 String value = output2015Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2015Map.put(avg, value);
	        	 }
	        	 else
		     		output2015Map.put(avg, job);
        	}
        	else
        	{
	        	 if(output2016Map.containsKey(avg))
	        	 {
	        		 String value = output2016Map.get(avg);
	        		 value = value + "\t" + job;
	        		 output2016Map.put(avg, value);
	        	 }
	        	 else
		     		output2016Map.put(avg, job);
        	}
	      }
       
 	     
 	      
 	     protected void cleanup(Context context) throws IOException,
 	      InterruptedException {
 	      	  context.write(new Text("Year"), new Text(nYear));
 	      	  printMapOutput(nYear, context);
 	      }	
 	     private void printMapOutput( String year, Context context) throws IOException, InterruptedException{
 	    	  Map<Double, String> map;
 		         if(year.equals("2011"))
 		         {
 		        	 map = output2011Map;
 		         }
 		         else if(nYear.equals("2012")){
 		        	 map = output2012Map;
 		         }
 		         else if(nYear.equals("2013")){
 		        	 map = output2013Map;
 		         }
 		         else if(nYear.equals("2014")){
 		        	 map = output2014Map;
 		         }
 		         else if(nYear.equals("2015")){
 		        	 map = output2015Map;
 		         }
 		         else
 		         {
 		        	 map = output2016Map;	 
 		         }
 	      	
 		      //   int i =0;
 		         for(Map.Entry<Double,String> entry : map.entrySet()) {
 		        	 
 		        	  Double mapkey = entry.getKey();
 		        	  String value = entry.getValue();
 		        	  context.write(new Text(value.toString()), new Text(mapkey.toString()));
 		        //	  i++;
 		        //	  if(i > 4)
 		        //		  break;
 		         }
 	      
 		   }
}
	   
	   //Partitioner class
		
	   public static class CaderPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	    	  String[] str = value.toString().trim().split("\\t",0);
		         String year = str[1].trim();
	         
	         if(year.equals("2011"))
	         {
	            return 0;
	         }
	         else if(year.equals("2012"))
	         {
	            return 1 ;
	         }
	         else if(year.equals("2013"))
	         {
	            return 2 ;
	         }
	         else if (year.equals("2014"))
	         {
	            return 3;
	         }
	         else if (year.equals("2015"))
	         {
	            return 4;
	         }
	         else 
	         {
	            return 5;
	         }

	      }
	   }
	   public int run(String[] arg) throws Exception
	   {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(AvgYearlyWageAggregator.class);
		  job.setJobName("Average wage for Full time employers");
	      FileInputFormat.setInputPaths(job, new Path(arg[0]));
	      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
			
	      job.setMapperClass(MapClass.class);
			
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      
	      //set partitioner statement
			
	    job.setPartitionerClass(CaderPartitioner.class);
	     
	      job.setReducerClass(ReduceClass.class);
	      job.setNumReduceTasks(6);
	      job.setInputFormatClass(TextInputFormat.class);
			
	      job.setOutputFormatClass(TextOutputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	   }
	   
	   public static void main(String ar[]) throws Exception
	   {
	      ToolRunner.run(new Configuration(), new AvgYearlyWageAggregator(),ar);
	      System.exit(0);
	   }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

}

/*
 * f you require a Descending IntWritable Comparable, you can create one and use it like this -

job.setSortComparatorClass(DescendingIntComparable.class);

In case if you are using JobConf, use this to set

jobConfObject.setOutputKeyComparatorClass(DescendingIntComparable.class);

Put the following code below your main() function -

public static void main(String[] args) {
    int exitCode = ToolRunner.run(new YourDriver(), args);
    System.exit(exitCode);
}

//this class is defined outside of main not inside
public static class DescendingIntWritableComparable extends IntWritable {
    /** A decreasing Comparator optimized for IntWritable.  
    public static class DecreasingComparator extends Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}
 * */
