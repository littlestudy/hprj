package org.stu.example;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.stu.example.gen.Stock;

import com.google.common.collect.Lists;

public class AvroStockUtils {
	
	public static List<Stock> fromCsvFile(File file) throws IOException{
		return fromCsvStream(FileUtils.openInputStream(file));
	}
	
	public static List<Stock> fromCsvStream(InputStream is) throws IOException{
		List<Stock> stocks = Lists.newArrayList();
		for (String line : IOUtils.readLines(is))
			stocks.add(fromCsv(line));
		is.close();
		return stocks;
	}
	
	public static Stock fromCsv(String line){
		String parts[] = line.split(",", -1);
		Stock stock = new Stock();
		
		stock.setSymbol(parts[0]);
		stock.setDate(parts[1]);
		stock.setOpen(Double.valueOf(parts[2]));
		stock.setHigh(Double.valueOf(parts[3]));
		stock.setLow(Double.valueOf(parts[4]));
		stock.setClose(Double.valueOf(parts[5]));
		stock.setVolume(Integer.valueOf(parts[6]));
		stock.setAdjClose(Double.valueOf(parts[7]));
		
		return stock;
	}
}

