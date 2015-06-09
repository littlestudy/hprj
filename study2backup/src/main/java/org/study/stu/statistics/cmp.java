package org.study.stu.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class cmp {
	public static void main(String[] args) throws IOException {
		long amount = 4000000;
		String o1 = "/home/ym/data/4m-r-200k/part-r-00000";
		String o2 = "/home/ym/data/4m-csv7/part-r-00000";
		
		String d1 = "/home/ym/data/diff/aa";
		String d2 = "/home/ym/data/diff/bb";
		
		String same1 = "/home/ym/data/diff/same1";
		String same2 = "/home/ym/data/diff/same2";
				
		BufferedReader reader1 = new BufferedReader(new FileReader(o1));
		BufferedReader reader2 = new BufferedReader(new FileReader(o2));
		PrintStream ps1 = new PrintStream(d1);
		PrintStream ps2 = new PrintStream(d2);
		PrintStream sps1 = new PrintStream(same1);
		PrintStream sps2 = new PrintStream(same2);
		//String s1 = reader1.readLine();
		//String s2 = reader2.readLine();
		
		//System.out.println(s1);
		//System.out.println(s2);
		
		String s1 = null;
		String s2 = null;
		long i = 0;
		long j = 0;
		for (i = 0; i < amount; i++){	
			s1 = reader1.readLine();
			s2 = reader2.readLine();
			if (!s1.equals(s2)){
				j++;
				ps1.println(s1);
				ps2.println(s2);
			} else {
				sps1.println(s1);
				sps2.println(s2);
			}
		}
		
		System.out.println("i " + i);
		System.out.println("j " + j);
		
		reader1.close();
		reader2.close();
		ps1.close();
		ps2.close();
		sps1.close();
		sps2.close();
	}
}
