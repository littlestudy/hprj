package org.v1.utils.im.rimwd;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.TestConstant;

public class RestoreInMemoryWithDictTest {

	private List<String[]> groups;
	private List<String> dictStrs;
	private List<String> records;
	
	@Before
	public void setUp() throws Exception {
		String dbtestFile = TestConstant.TEST_RESOURCES_DIR + "/dbtest.txt";
		LineIterator iter = IOUtils.lineIterator(new FileReader(dbtestFile));
				
		int amount = 0;
		
		groups = new ArrayList<String[]>();
		iter.hasNext();
		amount = Integer.valueOf(iter.next());
		for (int i = 0;  i < amount; i++)
			if (iter.hasNext())
			groups.add(iter.next().split(",", -1));
		
		dictStrs = new ArrayList<String>();
		iter.hasNext();
		amount = Integer.valueOf(iter.next());
		for (int i = 0; i < 6; i++){
			if (iter.hasNext())
				dictStrs.add(iter.next());
		}
		
		records = new ArrayList<String>();
		iter.hasNext();
		amount = Integer.valueOf(iter.next());
		for (int i = 0; i < amount; i++){
			if (iter.hasNext())
				records.add(iter.next());
		}
			
	}

	@Test
	public void testRestoreInMemory() {
		//show();
		RestoreInMemoryWithDict rimwd = new RestoreInMemoryWithDict(dictStrs, groups);
		//rimwd.getDictionaryBundle().showDictionaries();
		//rimwd.getGroupBundle().showGroupBundle();
		
		//System.out.println(records.get(0));
		List<String> list = rimwd.restoreInMemory(records.get(1));
		for (String str : list)
			System.out.println(str);
	}
	
	@SuppressWarnings("unused")
	private void show(){
		for (String[] strs : groups)
			System.out.println(StringUtils.join(strs, ","));		
		
		for (String str : dictStrs)
			System.out.println(str);
				
		for (String treerecord : records)
			System.out.println(treerecord);
	}

}
