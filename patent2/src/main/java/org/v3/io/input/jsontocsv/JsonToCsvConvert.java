package org.v3.io.input.jsontocsv;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.v3.common.GroupBundle;
import org.v3.io.input.BaseDataConvert;

public class JsonToCsvConvert extends BaseDataConvert{

	private JSONParser parser = new JSONParser();
	
	public JsonToCsvConvert(GroupBundle groupBundle) {
		super(groupBundle);
	}

	@Override
	public String dataFormat(String originalStr) {
		return jsonStringToCsv(originalStr);
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
				sb.append(tsb.toString().substring(1) + groupBundle.getSeparator());
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println("json to csv: " + sb.toString().substring(0, sb.toString().length() - 2));
		return sb.toString().substring(0, sb.toString().length() - groupBundle.getSeparator().length());
	}

	private String replaceStr(String line) {
		String[] strs = line.split(",");
		StringBuilder sb = new StringBuilder();
		for (String str : strs)
			sb.append("|" + str.trim());
		return sb.substring(1);
	}
}
