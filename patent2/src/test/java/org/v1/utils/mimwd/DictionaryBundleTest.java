package org.v1.utils.mimwd;

import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.DictionaryBundle;

public class DictionaryBundleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testFind() {
		DictionaryBundle dictionaryBundle = new DictionaryBundle(2);
		System.out.println(dictionaryBundle.find(0, "aa"));
		System.out.println(dictionaryBundle.find(0, "bb"));
		System.out.println(dictionaryBundle.find(0, "aa"));
		System.out.println(dictionaryBundle.find(1, "cc"));
	}

	@Test
	public void testSearch() {
		DictionaryBundle dictionaryBundle = new DictionaryBundle(2);
		System.out.println(dictionaryBundle.search(0, "aa"));
		System.out.println(dictionaryBundle.search(0, "aa"));
		dictionaryBundle.find(0, "aa");
		System.out.println(dictionaryBundle.search(0, "aa"));
		System.out.println(dictionaryBundle.search(1, "cc"));
	}

}
