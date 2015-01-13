package org.v1.patent.uncompress;

import java.util.ArrayList;
import java.util.List;

public class UnCompress {
	public static List<String> UncompressUtil(String records) {
		int curPos = 0;
		String field = null;
		List<String> result = new ArrayList<String>();
		List<String> fileds = new ArrayList<String>();
		while (curPos < records.length()) {
			field = getField(records, curPos);
			curPos += field.length();
			fileds.add(field.substring(1, field.length() - 1));
			if (records.charAt(curPos) == '[') {
				curPos++;
			} else if (records.charAt(curPos) == '(') {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
			} else {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
				while (curPos < records.length()
						&& records.charAt(curPos) == ']') {
					fileds.remove(fileds.size() - 1);
					curPos++;
				}
			}
		}
		return result;
	}

	private static String getField(String records, int startPos) {
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

	private static String buildObject(List<String> fields) {
		StringBuilder sb = new StringBuilder();
		for (String field : fields) {
			sb.append("," + field);
		}
		return sb.toString().substring(1);
	}
}