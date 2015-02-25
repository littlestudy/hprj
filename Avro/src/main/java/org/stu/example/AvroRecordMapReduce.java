package org.stu.example;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stu.example.gen.Stock;
import org.stu.example.gen.StockAvg;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

public class AvroRecordMapReduce extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String [] {				
				"/tmp/stocks.avro",
				"/tmp/stockavgoutput4"
		};
		int res = ToolRunner.run(new Configuration(),  new AvroRecordMapReduce(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf);
		job.setJarByClass(AvroRecordMapReduce.class);
		
		AvroJob.setInputSchema(job, Stock.SCHEMA$);
		AvroJob.setMapOutputSchema(job, Pair.getPairSchema(Schema.create(Schema.Type.STRING), Stock.SCHEMA$));
		AvroJob.setOutputSchema(job, StockAvg.SCHEMA$);
		
		AvroJob.setMapperClass(job, MapperClass.class);
		AvroJob.setReducerClass(job, ReducerClass.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		AvroJob.setOutputCodec(job, SNAPPY_CODEC);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return JobClient.runJob(job).isSuccessful() ? 0 : 1;
	}

	public static class MapperClass extends AvroMapper<Stock, Pair<Utf8, Stock>>{
		@Override
		public void map(Stock stock,
				AvroCollector<Pair<Utf8, Stock>> collector,
				Reporter reporter) throws IOException {
			collector.collect(new Pair<Utf8, Stock>(new Utf8(stock.getSymbol().toString()), stock));
		}
	}
	
	public static class ReducerClass extends AvroReducer<Utf8, Stock, StockAvg>{
		@Override
		public void reduce(Utf8 symbol, Iterable<Stock> stocks,
				AvroCollector<StockAvg> collector,
				Reporter reporter) throws IOException {
			Mean mean = new Mean();
			for (Stock stock : stocks)
				mean.increment(stock.getOpen());
			StockAvg avg = new StockAvg();
			avg.setSymbol(symbol.toString());
			avg.setAvg(mean.getResult());
			
			collector.collect(avg);
		}
	}
}
