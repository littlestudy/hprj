package org.v3.utils.im;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

public class RestoreInMemoryWithDictTest {
	
	private List<String[]> groups;
	private List<String> dictStrs;
	private List<String> records;
	private String separator;
	
	@Before
	public void setUp() throws Exception {
		String dbtestFile = "/home/ym/ytmp/hadoopexp/output/ccc/y1";
		LineIterator iter = IOUtils.lineIterator(new FileReader(dbtestFile));
				
		int amount = 0;
		
		groups = new ArrayList<String[]>();
		String[] amountAndSeparator = iter.next().split(",", -1);
		amount = Integer.valueOf(amountAndSeparator[0]);
		separator = amountAndSeparator[1];
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
		RestoreInMemoryWithDict rd = new RestoreInMemoryWithDict(dictStrs, groups, separator);
		//rd.getDictionaryBundle().showDictionaries();
		//rd.getGroupBundle().showGroupBundle();
		
		for (int i = 0; i < records.size(); i++){
			List<String> list = rd.restoreInMemory(records.get(i));
			for (String str : list)
				System.out.println(str);
		}
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
