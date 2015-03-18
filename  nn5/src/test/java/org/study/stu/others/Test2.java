package org.study.stu.others;

import java.io.IOException;
import java.io.PrintStream;

public class Test2 {
	
	public static void main(String[] args) throws IOException {
		PrintStream ps = new PrintStream("/home/ym/ytmp/testfile");
		long n = 1;		
		
		
		String str1 = createStr("0", 31);
		for (long i = 0; i < n; i++)
			ps.println(str1);
		String str2 = createStr("1", 31);
		for (long i = 0; i < n; i++)
			ps.println(str2);
		
		ps.close();
	}
	
	public static String createStr(String c, int num){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < num; i++)
			sb.append(c);
		return sb.toString();	
	}
}
