package org.v1.utils.mergeinmemory_v2;

import java.util.ArrayList;
import java.util.List;

public class GroupBundle {
	
	private class Group{
		private int mBaseFieldIndex;
		private String [] mGroup;
		
		public Group(String [] group, int baseFieldIndex){
			mGroup = group;
			mBaseFieldIndex = baseFieldIndex;
		}

		public int getBaseFieldIndex() {
			return mBaseFieldIndex;
		}

		public String[] getGroup() {
			return mGroup;
		}	
	}
	
	private int fieldAmount;
	private List<Group> bundle;
	
	public GroupBundle(List<String []> list){
		List<Group> tmpBundle = new ArrayList<Group>();
		bundle = new ArrayList<Group>();
		int amount = 0;		
		for (int i = list.size(); i > 0 ; i--){			
			tmpBundle.add(new Group(list.get(i - 1), amount));
			amount += list.get(i - 1).length;
		}
		
		fieldAmount = amount;
		for (int i = tmpBundle.size(); i > 0; i--)
			bundle.add(tmpBundle.get(i - 1));
	}

	public int getFieldAmount() {
		return fieldAmount;
	}
	
	public int getGroupIndexBase(int groupNumber){
		return bundle.get(groupNumber).getBaseFieldIndex();
	}
	
	public String[] getGroup(int groupNumber){
		return bundle.get(groupNumber).getGroup();
	}
	
	public int getGroupAmount(){
		return bundle.size();
	}
}
