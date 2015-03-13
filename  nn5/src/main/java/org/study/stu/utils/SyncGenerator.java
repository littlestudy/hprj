package org.study.stu.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class SyncGenerator {

	public static byte[] getMD5Sync(){
		try {
			MessageDigest digester = MessageDigest.getInstance("MD5");
			long time = System.currentTimeMillis();
			digester.update((UUID.randomUUID()+"@"+time).getBytes());
			return digester.digest();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	private final static String[] hexDigits = {
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"a", "b", "c", "d", "e", "f"};
	
	public static String byteArrayToHexString(byte[] b){  
		StringBuffer resultSb = new StringBuffer();  
        for (int i = 0; i < b.length; i++){  
        	resultSb.append(byteToHexString(b[i]));  
        }  
        return resultSb.toString();  
    }  
        
    private static String byteToHexString(byte b){  
    	int n = b;  
        if (n < 0)  
            n = 256 + n;  
        int d1 = n / 16;  
        int d2 = n % 16;  
        
        return hexDigits[d1] + hexDigits[d2];  
    }
}
