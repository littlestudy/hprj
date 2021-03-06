package org.study.stu.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.study.stu.common.dict.RDictionaryBundle;

public class RestoreInMemoryWithDict {
	
	private RDictionaryBundle dictionaryBundle;
	private GroupBundle groupBundle;
	
	private DataBlock block;
	
	List<String> result = new ArrayList<String>();
	
	public RestoreInMemoryWithDict(ByteBuffer gbBytes) throws IOException{
		groupBundle = new GroupBundle(gbBytes);
		dictionaryBundle = new RDictionaryBundle(groupBundle.getFieldAmount());		
	}
	
	public RestoreInMemoryWithDict(DataBlock block) throws IOException{
		this.block = block; 
		groupBundle = DataBlock.getGroupBundle();
		dictionaryBundle = block.getRDictionaryBundle();
	}
	
	public List<String> restore() throws IOException {
		for (String records: block.getRecords())
			restoreInMemory(records);
		
		return result;
	}
	
	public void restoreInMemory(String records) {
		int curPos = 0;
		
		int groupNumber = -1;
		String group = null;				
		Stack<String> groupStack = new Stack<String>();
		while (curPos < records.length()) {
			group = getField(records, curPos);
			groupNumber++;
			curPos += group.length();
			groupStack.push(restoreGroup(group, groupNumber));
			if (records.charAt(curPos) == '[') {
				curPos++;
			} else if (records.charAt(curPos) == '(') {
				result.add(buildObject(groupStack));				
				groupStack.pop();
				groupNumber--;
			} else if (records.charAt(curPos) == ']'){
				result.add(buildObject(groupStack));
				groupStack.pop();
				groupNumber--;
				while (curPos < records.length() && records.charAt(curPos) == ']') {
					groupStack.pop();
					groupNumber--;
					curPos++;
				}
			} else 
				System.out.println("error char.");
		}
	}

	private String restoreGroup(String group, int groupNumber) {		
		StringBuilder sb = new StringBuilder();		
		
		group = group.substring(1, group.length() - 1);
		int groupIndexBase = groupBundle.getGroupIndexBase(groupNumber);		
		String [] parts = group.split(",", -1);		
		for (int i = 0; i < parts.length; i++){
			int code = Integer.valueOf(parts[i]);
			String field = dictionaryBundle.search(groupIndexBase + i, code);
			sb.append(",").append(field);
		}
		return sb.substring(1);
	}

	private String getField(String records, int startPos) {
		int dep = 1;
		int searchPos = startPos + 1;
		while (dep > 0){
			if (records.charAt(searchPos) == '(')
				dep++;
			else if (records.charAt(searchPos) == ')')
				dep--;
			searchPos++;
		}
		
		return records.substring(startPos, searchPos);
	}

	private String buildObject(Stack<String> groupStack) {
		StringBuilder sb = new StringBuilder();
		for (String field : groupStack) {
			sb.append("," + field);
		}
		return sb.substring(1);
	}

	public RDictionaryBundle getDictionaryBundle() {
		return dictionaryBundle;
	}

	public void setDictionaryBundle(RDictionaryBundle dictionaryBundle) {
		this.dictionaryBundle = dictionaryBundle;
	}

	public GroupBundle getGroupBundle() {
		return groupBundle;
	}

	public void setGroupBundle(GroupBundle groupBundle) {
		this.groupBundle = groupBundle;
	}	
	
	public void setDictFromByteBuffer(int dictNum, ByteBuffer bb) throws IOException{
		dictionaryBundle.setDictFromByteBuffer(dictNum, bb);
	}
}