package org.v1.test.mergeinmemory.v2;

import java.util.ArrayList;
import java.util.List;

import org.v1.utils.mergeinmemory_v2.DataBlock;
import org.v1.utils.mergeinmemory_v2.MergeInMemoryWithDict;

public class MergeInMemoryWithDictTest {
	public static void main(String[] args) {
		String str1 = "0a##1b,2c,3d##4e,5f";		
		String str2 = "0a##1b,2c,3d##4d,5f";	
		String str3 = "0y##1t,2x,3d##4e,5t";	
		String str4 = "0y##1b,2x,3h##4e,5t";	
		String str5 = "0x##1q,2x,3d##4e,5t";	
		String str6 = "0x##1b,2x,3s##4r,5t";	
		String str7 = "0x##1w,2x,3d##4e,5i";	
		String str8 = "0z##1b,2x,3d##4e,5m";	
		List<String> values = new ArrayList<String>();
		values.add(str1);
		values.add(str2);
		values.add(str3);
		values.add(str4);
		values.add(str5);
		values.add(str6);
		values.add(str7);
		values.add(str8);

		
		String [] strs1 = new String[] {"aa", "bb"};
		String [] strs2 = new String[] {"cc", "dd", "ee"};
		String [] strs3 = new String[] {"ff"};
		List<String []> list = new ArrayList<String[]>();
		list.add(strs1);
		list.add(strs2);
		list.add(strs3);
		
		MergeInMemoryWithDict miwd = new MergeInMemoryWithDict(list);
		DataBlock db = miwd.mergeInMemory(values);
		showDataBlock(db);
	}

	private static void showDataBlock(DataBlock db) {
		db.showRecords();
		System.out.println("--------------------------------");
		db.showDictionaryBundle();
	}
}
