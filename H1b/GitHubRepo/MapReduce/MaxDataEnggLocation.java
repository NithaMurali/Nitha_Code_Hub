package H1BPetitionData;

import java.io.IOException;


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

public class MaxDataEnggLocation  extends Configured implements Tool{
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

	            String job_title = str[4].trim().toLowerCase();
	            String worksite = str[8].trim();
	            String year = str[7].trim();
	            String case_status = str[1].trim().toUpperCase();
	            if(job_title.contains("data engineer") && (case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN")))
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
		   Text nYear = new Text(); 
		     int  max = 0;
		    String worksite = "";
		   
		   
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         int count = 0;
	         
		         for (Text val : values)
		         {
		        	 count++; 
		        	 nYear = val;
		         }
	         	if(count > max)
	         	{
	         		max = count;
	         		worksite = key.toString();
	         		key.clear();
	         		count = 0;
	         	}
	        }
	      
	      
	      protected void cleanup(Context context) throws IOException,
          InterruptedException {
	    	  context.write(new Text(nYear), new Text(worksite + " " + max));
	      }
	   }
	   
	   public int run(String[] arg) throws Exception
	   {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(MaxDataEnggLocation.class);
		  job.setJobName("Location with max data engineer");
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
	      ToolRunner.run(new Configuration(), new MaxDataEnggLocation(),ar);
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

