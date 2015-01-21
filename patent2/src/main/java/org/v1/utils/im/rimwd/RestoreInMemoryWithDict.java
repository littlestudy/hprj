package org.v1.utils.im.rimwd;

import java.util.ArrayList;
import java.util.List;

import org.v1.utils.im.mimwd.DictionaryBundle;
import org.v1.utils.im.mimwd.GroupBundle;

public class RestoreInMemoryWithDict {
	
	private DictionaryBundle dictionaryBundle;
	private GroupBundle groupBundle;
	
	public List<String> UncompressUtil(String records) {
		List<String> result = new ArrayList<String>();
		
		int curPos = 0;
		int baseFieldNumber = 0;
		String field = null;		
		List<String> fileds = new ArrayList<String>();
		while (curPos < records.length()) {
			field = getField(records, curPos);
			curPos += field.length();
			baseFieldNumber = restoreFields(field.substring(1, field.length() - 1), baseFieldNumber, fileds);
			//fileds.add(rFields);
			if (records.charAt(curPos) == '[') {
				curPos++;
			} else if (records.charAt(curPos) == '(') {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
			} else if (records.charAt(curPos) == ']'){
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
				while (curPos < records.length()
						&& records.charAt(curPos) == ']') {
					fileds.remove(fileds.size() - 1);
					curPos++;
				}
			} else 
				System.out.println("error char.");
		}
		return result;
	}

	private int restoreFields(String subFields, int baseFieldNumber,
			List<String> fileds) {
		StringBuilder sb = new StringBuilder();
		
		String [] parts = subFields.split(",", -1);		
		for (int i = 0; i < parts.length; i++){
			
		}
		return baseFieldNumber + parts.length;  //////////
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

	private String buildObject(List<String> fields) {
		StringBuilder sb = new StringBuilder();
		for (String field : fields) {
			sb.append("," + field);
		}
		return sb.toString().substring(1);
	}
}