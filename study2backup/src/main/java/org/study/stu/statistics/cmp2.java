package org.study.stu.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class cmp2 {
	public static void main(String[] args) throws IOException {
		long amount = 1426;
		String o1 = "/home/ym/data/4m-r-200k/part-r-00000";
		String o2 = "/home/ym/data/4m-csv7/part-r-00000";
		
		String d1 = "/home/ym/data/diff/a";
		String d2 = "/home/ym/data/diff/b";
				
		BufferedReader reader1 = new BufferedReader(new FileReader(d1));
		BufferedReader reader2 = new BufferedReader(new FileReader(d2));
		//PrintStream ps1 = new PrintStream(d1);
		//PrintStream ps2 = new PrintStream(d2);
		
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
			reader2 = new BufferedReader(new FileReader(d2)); 
			for (j = 0; j < amount; j++) {
				s2 = reader2.readLine();
				if (s1.equals(s2)){
					break;
				}	
			}
			
			if (j >= amount)
				System.out.println("not find");
		}
		
		reader1.close();
		reader2.close();
		//ps1.close();
		//ps2.close();
	}
}
