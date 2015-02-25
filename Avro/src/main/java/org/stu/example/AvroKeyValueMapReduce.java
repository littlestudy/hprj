package org.stu.example;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stu.example.gen.Stock;
import org.stu.example.gen.StockAvg;

public class AvroKeyValueMapReduce extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String[] {
			"/tmp/stocks.kv.avro",
			"/tmp/stockskvoutput2",
		};
		int res = ToolRunner.run(new Configuration(), new AvroKeyValueMapReduce(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(AvroKeyValueMapReduce.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Stock.SCHEMA$);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);
				
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputValueSchema(job, StockAvg.SCHEMA$);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MapperClass 
		extends Mapper<AvroKey<CharSequence>, AvroValue<Stock>, Text, DoubleWritable>{
		
		@Override
		protected void map(
				AvroKey<CharSequence> key,
				AvroValue<Stock> value,
				Context context) throws IOException, InterruptedException {
			context.write(new Text(key.toString()), new DoubleWritable(value.datum().getOpen()));
		}
	}
	
	public static class ReducerClass 
		extends Reducer<Text, DoubleWritable, Text, AvroValue<StockAvg>>{
		
		@Override
		protected void reduce(
				Text key,
				Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			Mean mean = new Mean();
			for (DoubleWritable val : values)
				mean.increment(val.get());
			StockAvg avg = new StockAvg();
			avg.setSymbol(key.toString());
			avg.setAvg(mean.getResult());
			context.write(key, new AvroValue<StockAvg>(avg));
		}
	}
}
