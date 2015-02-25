package org.stu.example;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.stu.example.gen.Stock;
import org.stu.utils.Base;
import org.stu.utils.Constant;

public class AvroStockFileWrite extends Base{
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
				Constant.DEFAULT_RESOURCES_DIR + "/stocks.txt",
				"/tmp/stocks.avro"
		};
		exec(new AvroStockFileWrite(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		File inputFile = new File(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();

		FileSystem hdfs = FileSystem.get(conf);
		
		OutputStream os = hdfs.create(outputPath);
		writeToAvro(inputFile, os);
		
		return 0;
	}

	public static void writeToAvro(File inputFile, OutputStream outputStream) throws IOException{
		DataFileWriter<Stock> writer = new DataFileWriter<Stock>(new SpecificDatumWriter<Stock>());
		
		writer.setCodec(CodecFactory.snappyCodec());
		writer.create(Stock.SCHEMA$, outputStream);
		
		for (Stock stock : AvroStockUtils.fromCsvFile(inputFile))
			writer.append(stock);
		
		IOUtils.closeStream(writer);
		IOUtils.closeStream(outputStream);
	}
}




















