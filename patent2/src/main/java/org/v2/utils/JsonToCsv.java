package org.v2.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.v2.common.GroupBundle;

public class JsonToCsv {
	private String groupSeparator;
	private GroupBundle groupBundle;
	private JSONParser parser = new JSONParser();
	
	public JsonToCsv(GroupBundle groupBundle) {
		this.groupBundle = groupBundle;
	}
	
	public void setGroupSeparator(String groupSeparator){
		this.groupSeparator = groupSeparator;
	}

	public String jsonStringToCsv(String jsonString) {
		StringBuilder sb = new StringBuilder();
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(jsonString);
			for (int i = 0; i < groupBundle.getGroupAmount(); i++) {
				String[] fieldGroup = groupBundle.getGroup(i);
				StringBuilder tsb = new StringBuilder();
				for (int j = 0; j < fieldGroup.length; j++) {
					Object field = jsonObj.get(fieldGroup[j]);
					if (field == null || "".equals(field)) {
						field = "";
						tsb.append("," + field.toString());
					} else {
						String fieldStr = field.toString();
						if (fieldStr.contains(","))
							fieldStr = replaceStr(fieldStr);
						tsb.append("," + fieldStr);
					}
				}
				sb.append(tsb.toString().substring(1) + groupSeparator);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		//System.out.println("json to csv: " + sb.toString().substring(0, sb.toString().length() - 2));
		return sb.toString().substring(0, sb.toString().length() - groupSeparator.length());
	}

	private String replaceStr(String line) {
		String[] strs = line.split(",");
		StringBuilder sb = new StringBuilder();
		for (String str : strs)
			sb.append("|" + str.trim());
		return sb.substring(1);
	}
}
