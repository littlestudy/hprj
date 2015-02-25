package org.stu.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stu.example.gen.Stock;
import org.stu.example.gen.StockAvg;

public class AvroMixedMapReduce extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String [] {
				"/tmp/stocks.avro",
				"/tmp/stockavgoutput2",
		};
		int res = ToolRunner.run(new Configuration(),  new AvroMixedMapReduce(), args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf);
		job.setJarByClass(AvroMixedMapReduce.class);
		
		job.set(AvroJob.INPUT_SCHEMA, Stock.SCHEMA$.toString());
		job.set(AvroJob.OUTPUT_SCHEMA, StockAvg.SCHEMA$.toString());
		job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());
		
		job.setInputFormat(AvroInputFormat.class);
		job.setOutputFormat(AvroOutputFormat.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return JobClient.runJob(job).isSuccessful() ? 0 : 1;
	}
	
	public static class MapperClass 
		extends MapReduceBase implements Mapper<AvroWrapper<Stock>, NullWritable, Text, DoubleWritable>{

		@Override
		public void map(AvroWrapper<Stock> key,
						 NullWritable value,
						 OutputCollector<Text, DoubleWritable> output, 
						 Reporter reporter)
				throws IOException {
			output.collect(new Text(key.datum().getSymbol().toString()),
							new DoubleWritable(key.datum().getOpen()));
		}		
	}

	public static class ReducerClass 
		extends MapReduceBase implements Reducer<Text, DoubleWritable, AvroWrapper<StockAvg>, NullWritable>{

		@Override
		public void reduce(Text key, 
				Iterator<DoubleWritable> values,
				OutputCollector<AvroWrapper<StockAvg>, NullWritable> output,
				Reporter reporter) throws IOException {
			Mean mean  = new Mean();
			while (values.hasNext())
				mean.increment(values.next().get());
			
			StockAvg avg = new StockAvg();
			avg.setSymbol(key.toString());
			avg.setAvg(mean.getResult());
			output.collect(new AvroWrapper<StockAvg>(avg), NullWritable.get());
		}		
	}
}
