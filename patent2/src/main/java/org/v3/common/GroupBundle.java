package org.v3.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.v2.utils.Constant;

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
		
		public int getFieldNumber(String field){
			for (int i = 0; i < mGroup.length; i++)
				if (mGroup[i].equals(field))
					return i + mBaseFieldIndex;
			return -1;
		}
	}
	
	private int fieldAmount;
	private List<Group> bundle;
	private String separator;
	
	
	/**
	 * 归并按原记录顺序从后向前进行，如原记录顺数：array1 ## array2 ## array3 ## array4
	 * 
	 * list中是按需要归并的字段组顺序：array4, array3, array2, array1，（为了方便用户）
	 * 
	 * GroupBundle中的顺序和原记录顺序一样，array1, array2, array3, array4
	 */
	public GroupBundle(List<String []> list){
		this(list, Constant.DEFAULT_SEPARATOR);
	}
	
	public GroupBundle(List<String []> list, String separator){
		bundle = new ArrayList<Group>();
		int amount = 0;		
		for (int i = list.size(); i > 0 ; i--){			
			bundle.add(new Group(list.get(i - 1), amount));
			amount += list.get(i - 1).length;
		}
		
		fieldAmount = amount;
		this.separator = separator;
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
	
	public int getFieldNumber(String field){
		int number = -1;
		for (int i = 0; i < bundle.size(); i++){
			number = bundle.get(i).getFieldNumber(field);
			if (number >= 0)
				break;
		}
			
		return number;
	}
	
	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public void showGroupBundle(){
		for (int i = 0; i < bundle.size(); i++)
			System.out.println(StringUtils.join(bundle.get(i).getGroup(), ","));
	}
}
