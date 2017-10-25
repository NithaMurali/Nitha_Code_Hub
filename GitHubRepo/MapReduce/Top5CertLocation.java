
package H1BPetitionData;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class Top5CertLocation  extends Configured implements Tool{
	 //Map class

	   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().trim().split("\t",0);
	       	 //(1)s_no int,2 case_status string,3 employer_name string,4 soc_name string,
	            //5 job_title string,6 full_time_position string,7 prevailing_wage int,
	            //8 year string,9 worksite string, 10 longitude double,11 latitude double)
	            //8 year string

	            String case_status = str[1].trim();
	            String worksite = str[8].trim();
	            String year = str[7].trim();
	          //  context.write(new Text(worksite), new Text(case_status + " " +year));
	            if(((case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))))
	            	context.write(new Text(worksite), new Text(year));
	           
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   public static class CaderPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	    	  
		         String year = value.toString().trim();
	         
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
	   
	   //Reducer class
		
	   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		   String nYear = ""; 
		     
		    String worksite = "";
		    public static Map<Integer,String> output2011Map = new TreeMap<Integer,String>(Collections.reverseOrder());
		    public static Map<Integer,String> output2012Map = new TreeMap<Integer,String>(Collections.reverseOrder());
		    public static Map<Integer,String> output2013Map = new TreeMap<Integer,String>(Collections.reverseOrder());
		    public static Map<Integer,String> output2014Map = new TreeMap<Integer,String>(Collections.reverseOrder());
		    public static Map<Integer,String> output2015Map = new TreeMap<Integer,String>(Collections.reverseOrder());
		    public static Map<Integer,String> output2016Map = new TreeMap<Integer,String>(Collections.reverseOrder());

	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         int count = 0;
	         worksite = key.toString();
	         for (Text val : values)
	         {
	        	 count++; 
	        	 nYear = val.toString().trim();
	         }
	         if(nYear.equals("2011"))
	         {
	        	 if(output2011Map.containsKey(count))
	        	 {
	        		 String value = output2011Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2011Map.put(count, value);
	        	 }
	        	 else
		     		output2011Map.put(count, worksite);
	         }
        	else if(nYear.equals("2012")){
	        	 if(output2012Map.containsKey(count))
	        	 {
	        		 String value = output2012Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2012Map.put(count, value);
	        	 }
	        	 else
		     		output2012Map.put(count, worksite);

         	}
         	else if(nYear.equals("2013")){
	        	 if(output2013Map.containsKey(count))
	        	 {
	        		 String value = output2013Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2013Map.put(count, value);
	        	 }
	        	 else
		     		output2013Map.put(count, worksite);

         	}
         	else if(nYear.equals("2014")){
	        	 if(output2014Map.containsKey(count))
	        	 {
	        		 String value = output2014Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2014Map.put(count, value);
	        	 }
	        	 else
		     		output2014Map.put(count, worksite);

         	}
         	else if(nYear.equals("2015")){
	        	 if(output2015Map.containsKey(count))
	        	 {
	        		 String value = output2015Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2015Map.put(count, value);
	        	 }
	        	 else
		     		output2015Map.put(count, worksite);

         	}
         	else
         	{
	        	 if(output2016Map.containsKey(count))
	        	 {
	        		 String value = output2016Map.get(count);
	        		 value = value + "\t" + worksite;
	        		 output2016Map.put(count, value);
	        	 }
	        	 else
		     		output2016Map.put(count, worksite);

         	}
         	

        }
	      
	      
      protected void cleanup(Context context) throws IOException,
      InterruptedException {
      	  context.write(new Text("Year"), new Text(nYear));
      	  printMapOutput(nYear, context);
      }	
      
      private void printMapOutput( String year, Context context) throws IOException, InterruptedException{
    	  Map<Integer, String> map;
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
      	
	         int i =0;
	         for(Map.Entry<Integer,String> entry : map.entrySet()) {
	        	 
	        	  Integer mapkey = entry.getKey();
	        	  String value = entry.getValue();
	        	  context.write(new Text(value.toString()), new Text(mapkey.toString()));
	        	  i++;
	        	  if(i > 4)
	        		  break;
	         }
      
	   }
	   }
	   
	   public int run(String[] arg) throws Exception
	   {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(Top5CertLocation.class);
		  job.setJobName("Top 5 Location with certified petitions");
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
	      ToolRunner.run(new Configuration(), new Top5CertLocation(),ar);
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

