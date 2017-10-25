
package H1BPetitionData;
import java.io.*;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;


import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class YearlyPetitionAggregator extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().trim().split("\\t",0);
       	 //(1)s_no int,2 case_status string,3 employer_name string,4 soc_name string,5 job_title string,6 full_time_position string,7 prevailing_wage int,8 year string,9 worksite string, 10 longitude double,11 latitude double)
            //8 year string
            String case_status = str[1].trim();
            String year=str[7].trim();
           context.write(new Text(case_status), new Text(year));
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
      private Double count = 0.0;
      private Double total2011 = new Double(0.0);
      private Double total2012 = new Double(0.0);
      private Double total2013 = new Double(0.0);
      private Double total2014 = new Double(0.0);
      private Double total2015 = new Double(0.0);
      private Double total2016 = new Double(0.0);
	    private Map<String,Double> output2011Map = new TreeMap<String,Double>();
	    private Map<String,Double> output2012Map = new TreeMap<String,Double>();
	    private Map<String,Double> output2013Map = new TreeMap<String,Double>();
	    private Map<String,Double> output2014Map = new TreeMap<String,Double>();
	    private Map<String,Double> output2015Map = new TreeMap<String,Double>();
	    private Map<String,Double> output2016Map = new TreeMap<String,Double>();
	    String nYear = "";
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
         count = 0.0;
        for (Text val : values)
         {
        	 nYear = val.toString();
        	 count++;  
         }
       if(nYear.equals("2011")){
        	total2011 = total2011 + count;
        	output2011Map.put(key.toString(), count);
        }
        else if(nYear.equals("2012")){
        	total2012 = total2012 + count;
        	output2012Map.put(key.toString(), count);
        }
        else if(nYear.equals("2013")){
        	total2013 = total2013 + count;
        	output2013Map.put(key.toString(), count);
        }
        else if(nYear.equals("2014")){
        	total2014 = total2014 + count;
        	output2014Map.put(key.toString(), count);
        }
        else if(nYear.equals("2015")){
        	total2015 = total2015+ count;
        	output2015Map.put(key.toString(), count);
        }
        else {
        	total2016 = total2016 + count;
        	output2016Map.put(key.toString(), count);
        }
   }
      
  protected void cleanup(Context context) throws IOException,
  InterruptedException {
  	  context.write(new Text("Year"), new Text(nYear));
  	  printMapOutput(nYear, context);
  }	
  
  private void printMapOutput( String year, Context context) throws IOException, InterruptedException{
	  Map<String, Double> map;
	  Double total;
         if(year.equals("2011"))
         {
        	 map = output2011Map;
        	 total = total2011;
         }
         else if(nYear.equals("2012")){
        	 map = output2012Map;
        	 total = total2012;
         }
         else if(nYear.equals("2013")){
        	 map = output2013Map;
        	 total = total2013;
         }
         else if(nYear.equals("2014")){
        	 map = output2014Map;
        	 total = total2014;
         }
         else if(nYear.equals("2015")){
        	 map = output2015Map;
        	 total = total2015;
         }
         else
         {
        	 map = output2016Map;
        	 total = total2016;
         }
  	
         for(Map.Entry<String, Double> entry : map.entrySet()) {
        	 
        	  String mapkey = entry.getKey();
        	  Double value = entry.getValue();
        	  context.write(new Text(mapkey), new Text(value.toString()));
        	//  context.write(new Text(mapkey), new Text(value.toString()+ "/"+ total + " = "+ Double.toString(((value/total) * 100)) + "%"));
              DecimalFormat df = new DecimalFormat("#.##");
              
        	  context.write(new Text(mapkey), new Text(df.format(((value/total) * 100)) + "%"));
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
	         String year = value.toString();
	         
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
	  job.setJarByClass(YearlyPetitionAggregator.class);
	  job.setJobName("Yearly petition count");
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
      ToolRunner.run(new Configuration(), new YearlyPetitionAggregator(),ar);
      System.exit(0);
   }
}














