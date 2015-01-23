package org.v1.utils.im.rimwd;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RDictionaryBundleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testSearch() {
		fail("Not yet implemented");
	}

	@Test
	public void testAddDictionary() {
		String dictionaryStr0 = "0p,0,0a,1";
		String dictionaryStr2 = ",0,2x,2,2c,1";
		RDictionaryBundle rDictionaryBundle = new RDictionaryBundle();
		rDictionaryBundle.addDictionary(0, dictionaryStr0);
		rDictionaryBundle.addDictionary(2, dictionaryStr2);
		rDictionaryBundle.showDictionaries();
		System.out.println("code: 1, field: " + rDictionaryBundle.search(0, 2));
	}

}
