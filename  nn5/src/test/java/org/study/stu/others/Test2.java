package org.study.stu.others;

import java.io.IOException;
import java.io.PrintStream;

public class Test2 {
	
	public static void main(String[] args) throws IOException {
		PrintStream ps = new PrintStream("/home/ym/ytmp/testfile");
		long n = 1000000;
		
		
		
		for (long i = 0; i < n; i++){
			//System.out.println(String.format("%1$060d", i));
			ps.println(String.format("%1$0100d", i));
		}
		System.out.println("ok");
		
		ps.close();
	}
	
	public static String createStr(String c, int num){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < num; i++)
			sb.append(c);
		return sb.toString();	
	}
}
