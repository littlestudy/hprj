package org.v1.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.v1.utils.MergeInMemory;

public class MergeInMemoryTest {
	
	private static final String OUTPUT_FILE = "D:\\develop\\data\\test";
	
	public static void main(String[] args) throws Exception {
		LineIterator iterator = IOUtils.lineIterator(new FileInputStream(OUTPUT_FILE), "UTF8");
		reduce(null, iterator, null);
	}
	
	protected static void reduce(Text key, LineIterator values, Context context)
		throws IOException, InterruptedException {
		List<String> list = new ArrayList<String>();
		while (values.hasNext())
			list.add(values.next());		
		
		List<String> results = MergeInMemory.mergeInMemory(list);
		for (String str : results)
			System.out.println(str);
	}
	
	static class Text {
		String value;

		public String get() {
			return value;
		}

		public void set(String value) {
			this.value = value;
		}	
	}

	class Context {
		public void write(Text key, Text value){
			
		}
	}
}
