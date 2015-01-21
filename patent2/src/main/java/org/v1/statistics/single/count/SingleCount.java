package org.v1.statistics.single.count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.v1.ioformat.JsonToCsvInputForamt;
import org.v1.utils.Constant;

public class SingleCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception {	
		args = new String[] {
				"hdfs://master:9000/user/hadoop/data/o100R",
				"/home/htmp/output/test"
		};
		int res = ToolRunner.run(new Configuration(),  new SingleCount(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SingleCount.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		JsonToCsvInputForamt.setGroups(Constant.getTargetFileds());
		JsonToCsvInputForamt.setGroupSeparator(",");
		job.setInputFormatClass(JsonToCsvInputForamt.class);

		job.setMapOutputKeyClass(SingleKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(SinglePartitioner.class);
		job.setSortComparatorClass(SingleComparator.class);
		job.setGroupingComparatorClass(SingleComparator.class);		

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		return 0;
	}
	
	
	public static class MappClass extends
			Mapper<LongWritable, Text, SingleKey, IntWritable> {
		private SingleKey fieldValueKey = new SingleKey();
		private IntWritable account = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",", -1);
			for (int i = 0; i < fields.length; i++) {
				String field = String.format("%1$02d", i + 1);
				fieldValueKey.set(field, fields[i]);
				context.write(fieldValueKey, account);
			}
		}
	}

	public static class ReduceClass extends
			Reducer<SingleKey, IntWritable, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private long total = 0;
		
		@Override
		protected void reduce(SingleKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			long account = 0;
			for (IntWritable i : values) {
				account += i.get();
			}
			total += (account * key.getValue().getBytes().length);
			outputKey.set(key.toString());
			outputValue.set(String.valueOf(account));
			context.write(outputKey, outputValue);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("total -->>> " + total + "\n");
		}
	}
}
