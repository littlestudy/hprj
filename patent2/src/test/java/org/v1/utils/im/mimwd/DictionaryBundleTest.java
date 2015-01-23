package org.v1.utils.im.mimwd;

import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.CDictionaryBundle;

public class DictionaryBundleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testFind() {
		CDictionaryBundle dictionaryBundle = new CDictionaryBundle(2);
		System.out.println(dictionaryBundle.find(0, "aa"));
		System.out.println(dictionaryBundle.find(0, "bb"));
		System.out.println(dictionaryBundle.find(0, "aa"));
		System.out.println(dictionaryBundle.find(1, "cc"));
	}

	@Test
	public void testSearch() {
		CDictionaryBundle dictionaryBundle = new CDictionaryBundle(2);
		System.out.println(dictionaryBundle.search(0, "aa"));
		System.out.println(dictionaryBundle.search(0, "aa"));
		dictionaryBundle.find(0, "aa");
		System.out.println(dictionaryBundle.search(0, "aa"));
		System.out.println(dictionaryBundle.search(1, "cc"));
	}

	@Test
	public void testDictionaryToString() {
		CDictionaryBundle dictionaryBundle = new CDictionaryBundle(2);
		System.out.println(dictionaryBundle.find(0, "aa"));
		System.out.println(dictionaryBundle.find(0, "bb"));
		System.out.println(dictionaryBundle.find(0, "cc"));
		System.out.println(dictionaryBundle.find(1, "dd"));
		System.out.println(dictionaryBundle.find(1, "ee"));
		
		//dictionaryBundle.showDictionaries();
		
		System.out.println(dictionaryBundle.dictionaryToString(0));
		System.out.println(dictionaryBundle.dictionaryToString(1));
	}
}
