package org.study.stu.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class Sample {
	public static void main(String[] args) throws IOException {
		long amount = 4000000;
		
		BufferedReader reader = new BufferedReader(new FileReader("/home/ym/Lstudy/data/0701"));
		PrintStream ps = new PrintStream("/home/ym/data/4m");
		for (long i = 0; i < amount; i++){	
			ps.println(reader.readLine());
		}
		reader.close();
		ps.close();
	}
}
