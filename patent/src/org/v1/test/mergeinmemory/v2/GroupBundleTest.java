package org.v1.test.mergeinmemory.v2;

import java.util.ArrayList;
import java.util.List;

import org.v1.utils.mergeinmemory_v2.GroupBundle;

public class GroupBundleTest {
	
	
	public static void main(String[] args) {
		
		String [] strs1 = new String[] {"aa", "bb"};
		String [] strs2 = new String[] {"cc", "dd", "ee"};
		String [] strs3 = new String[] {"ff"};
		
		List<String []> list = new ArrayList<String[]>();
		list.add(strs1);
		list.add(strs2);
		list.add(strs3);
		
		GroupBundle groupBundle = new GroupBundle(list);
		
		System.out.println("field amount: " + groupBundle.getFieldAmount());
		for (int i = 0; i < groupBundle.getGroupAmount(); i++){
			String group = "";
			for (String str : groupBundle.getGroup(i) )
				group += (str+" ");
			System.out.println("group " + i + " : " + group + ", base index: " + groupBundle.getGroupIndexBase(i));
		}
	}
}
