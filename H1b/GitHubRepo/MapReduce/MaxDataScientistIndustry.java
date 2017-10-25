package H1BPetitionData;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MaxDataScientistIndustry  extends Configured implements Tool{
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
	            String soc_name = str[3].trim();
	            if((job_title.contains("data scientist")))
	            	context.write(new Text(soc_name), new Text("One"));
	           
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
	      public int count = 0, max = 0;
	      private Text outputKey = new Text();
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         count = 0;
	         
	         for (Text val : values)
	         {	        	
	        	 count++;  
	         }
	         if(count > max)
	         {
	          max = count;
	          outputKey = key;
	         }
	//		context.write(outputKey, new Text(Integer.toString(count)));
	      }
	      
	      protected void cleanup(Context context) throws IOException,
          InterruptedException {
	    	  context.write(outputKey, new Text(Integer.toString(max)));
	      }
	   }
	   
	   public int run(String[] arg) throws Exception
	   {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(MaxDataScientistIndustry.class);
		  job.setJobName("Industry with max data scientist");
	      FileInputFormat.setInputPaths(job, new Path(arg[0]));
	      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
			
	      job.setMapperClass(MapClass.class);
			
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      
	      
	      job.setReducerClass(ReduceClass.class);
	      job.setInputFormatClass(TextInputFormat.class);
			
	      job.setOutputFormatClass(TextOutputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
			
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	   }
	   
	   public static void main(String ar[]) throws Exception
	   {
	      ToolRunner.run(new Configuration(), new MaxDataScientistIndustry(),ar);
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
