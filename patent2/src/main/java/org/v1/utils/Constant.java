package org.v1.utils;

import java.util.ArrayList;
import java.util.List;

public class Constant {
		public static List<String[]> getTargetFileds() {

		List<String[]> list = new ArrayList<String[]>();
		String[] group1 = { "appKey", "appVersion", "dataType" };
		String[] group2 = { "city", "ip", "isp", "logCity", "logProvince" };
		String[] group3 = { "deviceCarrier", "deviceHashMac", "deviceIMEI",
				"deviceMacAddr", "deviceModel", "deviceNetwork", "deviceOs",
				"deviceOsVersion", "deviceResolution", "deviceUdid",
				"appChannel" };
		String[] group4 = { "userName" };
		String[] group5 = { "occurTime", "persistedTime" };
		String[] group6 = { "eventId", "costTime", "logSource" };
		String[] group7 = { "sessionStep" };
		list.add(group1);
		list.add(group2);
		list.add(group3);
		list.add(group4);
		list.add(group5);
		list.add(group6);
		list.add(group7);

		return list;
	}
}
