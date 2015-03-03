package org.study.stu.utils;

import org.junit.Test;

public class SyncGeneratorTest {

	@Test
	public void testGetMD5Sync() {
		byte[] test = SyncGenerator.getMD5Sync();
		System.out.println(test.length);
		System.out.println(SyncGenerator.byteArrayToHexString(test));
	}

	@Test
	public void testByteArrayToHexString() {
		
	}

}
