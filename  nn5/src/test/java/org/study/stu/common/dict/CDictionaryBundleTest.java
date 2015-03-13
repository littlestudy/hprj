package org.study.stu.common.dict;

import java.io.IOException;

import org.junit.Test;

public class CDictionaryBundleTest {

	@Test
	public void testSetDictFromByteBuffer() throws IOException {
		CDictionaryBundle bundle1 = new CDictionaryBundle(2);
		bundle1.find(0, "a");
		bundle1.find(0, "b");
		bundle1.find(0, "c");
		bundle1.find(1, "d");
		bundle1.find(1, "e");
		
		System.out.println(bundle1.getDictionaryAmount());
		bundle1.showDictionaries();
		
		CDictionaryBundle bundle2 = new CDictionaryBundle(2);
		bundle2.setDictFromByteBuffer(0, bundle1.getByteBufferFromDict(0));
		bundle2.setDictFromByteBuffer(1, bundle1.getByteBufferFromDict(1));
		System.out.println(bundle2.getDictionaryAmount());
		bundle2.showDictionaries();
		
	}

}
