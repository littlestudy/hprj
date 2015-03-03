package org.study.stu.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.v2.common.GroupBundle;
import org.v2.common.RDictionaryBundle;

public class RestoreInMemoryWithDict {
	
	private RDictionaryBundle dictionaryBundle = new RDictionaryBundle();
	private GroupBundle groupBundle;
	
	public RestoreInMemoryWithDict(List<String> dictStrs, List<String[]> groups, String separator){
		for (int i = 0; i < dictStrs.size(); i++)
			dictionaryBundle.addDictionary(i, dictStrs.get(i));
		groupBundle = new GroupBundle(groups, separator);
	}
	
	public List<String> restoreInMemory(String records) {
		List<String> result = new ArrayList<String>();
		
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
		return result;
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
}