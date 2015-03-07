package org.study.stu.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.study.stu.utils.Constant;

public class GroupBundle {
	
	private class Group{
		private int mBaseFieldIndex;
		private String [] mGroup;
		
		public Group(String groupStr, int baseFildIndex){
			mGroup = groupStr.split(",", -1);
			mBaseFieldIndex = baseFildIndex;
		}

		public int getBaseFieldIndex() {
			return mBaseFieldIndex;
		}

		public String[] getGroupArray() {
			return mGroup;
		}
		
		public int getFieldNumber(String field){
			for (int i = 0; i < mGroup.length; i++)
				if (mGroup[i].equals(field))
					return i + mBaseFieldIndex;
			return -1;
		}
		
		public String getGroup(){
			return StringUtils.join(mGroup, ",");
		}
		
		public int getGroupLength(){
			return mGroup.length;
		}
	}
	
	private int fieldAmount;
	private List<Group> bundle;
	private String groupSeparator;	
		
	/**
	 * groupBundleStr = array1##array2##array3##array4
	 * 
	 * 归并的字段组顺序：array4, array3, array2, array1
	 * 
	 * GroupBundle中的顺序和原记录顺序一样，array1, array2, array3, array4
	 */
	public GroupBundle(String groupBundleStr, String groupSeparator){
		init(groupBundleStr, groupSeparator);
	}
	
	public GroupBundle(ByteBuffer bb) throws IOException{
		byte[] gbBytes = new byte[bb.remaining()];
		bb.get(gbBytes, 0, bb.remaining());
		DataInput dataInput = new DataInputStream(new ByteArrayInputStream(gbBytes));
		this.groupSeparator = dataInput.readUTF();
		String groupBundleStr = dataInput.readUTF();
		init(groupBundleStr, groupSeparator);		
	}
	
	private void init(String groupBundleStr, String groupSeparator){
		this.groupSeparator = groupSeparator;
		
		String [] gbArray = groupBundleStr.split(groupSeparator, -1);
		bundle = new ArrayList<GroupBundle.Group>(gbArray.length);
		int amount = 0;
		for (int i = 0; i < gbArray.length; i++){
			Group group = new Group(gbArray[i], amount);
			bundle.add(group);
			amount += group.getGroupLength();
		}
		fieldAmount = amount;
	}
	
	/**
	 * 归并按原记录顺序从后向前进行，如原记录顺数：array1 ## array2 ## array3 ## array4
	 * 
	 * list中是按需要归并的字段组顺序：array4, array3, array2, array1，（为了方便用户）
	 * 
	 * GroupBundle中的顺序和原记录顺序一样，array1, array2, array3, array4
	 */
	@Deprecated
	public GroupBundle(List<String []> list){
		this(list, Constant.DEFAULT_SEPARATOR);
	}
	
	@Deprecated
	public GroupBundle(List<String []> list, String separator){
		bundle = new ArrayList<Group>();
		int amount = 0;		
		for (int i = list.size(); i > 0 ; i--){			
			bundle.add(new Group(StringUtils.join(list.get(i - 1), ","), amount));
			amount += list.get(i - 1).length;
		}
		
		fieldAmount = amount;
		this.groupSeparator = separator;
	}

	public int getFieldAmount() {
		return fieldAmount;
	}
	
	public int getGroupIndexBase(int groupNumber){
		return bundle.get(groupNumber).getBaseFieldIndex();
	}
	
	public String getGroup(int groupNumber){
		return bundle.get(groupNumber).getGroup();
	}
	
	public String[] getGroupArray(int groupNumber){
		return bundle.get(groupNumber).getGroupArray();
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
	
	public String getGroupSeparator() {
		return groupSeparator;
	}

	public void setGroupSeparator(String separator) {
		this.groupSeparator = separator;
	}

	public void showGroupBundle(){
		for (int i = 0; i < bundle.size(); i++)
			System.out.println(getGroupBundleStr());
	}
	
	public String getGroupBundleStr(){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bundle.size(); i++)
			sb.append(groupSeparator + bundle.get(i).getGroup());
		return sb.substring(groupSeparator.length());
	}
	
	public ByteBuffer toByteBuffer() throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
		dataOutput.writeUTF(groupSeparator);
		dataOutput.writeUTF(getGroupBundleStr());
		dataOutput = null;
		baos.close();
		return ByteBuffer.wrap(baos.toByteArray());
	}
	
}
