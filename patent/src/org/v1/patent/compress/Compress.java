package org.v1.patent.compress;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.v1.ioformat.JsonToCsvInputForamt;
import org.v1.ioformat.NoSeperatorTextOutputFormate;
import org.v1.utils.Constant;

public class Compress extends Configured implements Tool{
	
	public static final String separator = "##";		
	
	public static void main(String[] args) throws Exception {		
		args = new String[] {
				"hdfs://master:9000/user/hadoop/data/o100R",
				"/home/htmp/output/test-compress/compress"
		};
		int res = ToolRunner.run(new Configuration(),  new Compress(), args);
		System.exit(res);
	}	
	
		@Override
	public int run(String[] args) throws Exception {
			
		String input = args[0];
		String outputbase = args[1];
		
		input = firstProcess(input, outputbase);
		
		Configuration conf = new Configuration();
		Job job = null;		
		String output = outputbase;
		for (int i = 0; i < Constant.getTargetFileds().size() - 1; i++) {

			job = Job.getInstance(conf);
			initJob2(job);
			output = outputbase + i;
			setJobPath(job, input, output);
			job.waitForCompletion(true);
			input = output;
		}
		
		return 0;
	}
	
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String subStr = "";

			if (line.contains("[")) {
				subStr = line.substring(line.indexOf("["));
				line = line.substring(0, line.indexOf("["));
			}

			if (!line.contains(separator)) { // 对于第一个字段组的情况
				context.write(new Text("(" + line + ")" + subStr), new Text(""));
				return;
			}

			int lastindex = line.lastIndexOf(separator);
			String newKey = line.substring(0, lastindex);
			String newValue = "("
					+ line.substring(lastindex + separator.length()) + ")";
			context.write(new Text(newKey), new Text(newValue + subStr));
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString());
			}
			if (sb.toString().equals(""))
				context.write(key, new Text(sb.toString()));
			else
				context.write(key, new Text("[" + sb.toString() + "]"));
		}
	}

	private static String firstProcess(String input, final String output) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		initJob1(job);
		setJobPath(job, input, output + "p");
		job.waitForCompletion(true);
		
		return output + "p";
	}

	public static void setJobPath(Job job, String input, String output)
			throws Exception {
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
	}

	public static void initJob1(Job job) throws Exception {

		job.setJarByClass(Compress.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		JsonToCsvInputForamt.setGroups(Constant.getTargetFileds());
		JsonToCsvInputForamt.setGroupSeparator(separator);			
		job.setInputFormatClass(JsonToCsvInputForamt.class);
		job.setOutputFormatClass(NoSeperatorTextOutputFormate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
	
	public static void initJob2(Job job) throws Exception {

		job.setJarByClass(Compress.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NoSeperatorTextOutputFormate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}


}
