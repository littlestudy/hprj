package org.v1.utils.mimwd;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.GroupBundle;

public class GroupBundleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetGroup() {
		String [] array1 = new String [] {"aa", "bb"};
		String [] array2 = new String [] {"cc", "dd", "ee"};
		String [] array3 = new String [] {"ff"};
		
		List<String[]> list = new ArrayList<String[]>();
		list.add(array1);
		list.add(array2);
		list.add(array3);
		
		GroupBundle groupBundle = new GroupBundle(list);
		
		int fieldAmount = groupBundle.getFieldAmount();
		System.out.println("field amount: " + fieldAmount);
		int groupAmount = groupBundle.getGroupAmount();
		System.out.println("group amount: " + groupAmount);
		for (int i = 0; i < groupAmount; i++)
		System.out.println("group: " + i 
							+ " base index: " + groupBundle.getGroupIndexBase(i) 
							+ " array: " + fromStringArray(groupBundle.getGroup(i))); 
	}

	private String fromStringArray(String [] array){
		StringBuilder sb = new StringBuilder();
		for (String str : array)
			sb.append(",").append(str);
		return sb.substring(1);
	}
}
