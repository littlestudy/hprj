package org.v1.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.List;

import org.v1.patent.uncompress.UnCompress;


public class UncompressTest {

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader("/home/htmp/output/test-compress/compress5/part-r-00000"));
		String line = null;
		PrintStream ps = new PrintStream("/home/htmp/output/uncompressResult");
		while ((line = reader.readLine()) != null){
			List<String> list = UnCompress.UncompressUtil(line);
			printStrings(list, ps);
		}
		reader.close();
		ps.close();
	}

	private static void printStrings(List<String> list, PrintStream ps) {
		for (int i = 0; i < list.size(); i++)
			ps.println(list.get(i));
	}
}
