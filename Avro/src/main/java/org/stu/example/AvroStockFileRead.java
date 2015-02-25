package org.stu.example;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.stu.example.gen.Stock;
import org.stu.utils.Base;

public class AvroStockFileRead extends Base{

	public static void main(String[] args) throws Exception {
		args = new String [] {"/tmp/stocks.avro"};
		exec(new AvroStockFileRead(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Path inputFile = new Path(args[0]);
		
		Configuration conf = getConf();
		
		FileSystem hdfs = FileSystem.get(conf);
		
		InputStream is = hdfs.open(inputFile);
		dumpStream(is);
		
		return 0;
	}
	
	public static void dumpStream(InputStream is) throws IOException{
		DataFileStream<Stock> reader = new DataFileStream<Stock>(is,  new SpecificDatumReader<Stock>(Stock.class));
		for (Stock a : reader) 
			System.out.println(ToStringBuilder.reflectionToString(a, ToStringStyle.SIMPLE_STYLE));
		
		IOUtils.closeStream(is);
		IOUtils.closeStream(reader);
	}
}
