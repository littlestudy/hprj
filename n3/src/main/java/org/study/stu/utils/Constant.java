package org.study.stu.utils;

import java.util.ArrayList;
import java.util.List;

public class Constant {
	public static List<String[]> getNELogFiledGroup() {

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
	
	public static List<String[]> getTestGroups() {

		List<String[]> list = new ArrayList<String[]>();
		String[] group1 = { "field4", "field5" };
		String[] group2 = { "field1", "field2", "field3" };
		String[] group3 = { "field0" };
		list.add(group1);
		list.add(group2);
		list.add(group3);

		return list;
	}
		
	public static final String DEFAULT_SEPARATOR = "##";
	
	public static final String DEFAULT_RESOURCES_DIR = System.getProperty("user.dir") + "/src/test/resources/";
	
	public static final String TEST_GROUP_BUNDLE_STR = "field0##field1,field2,field3##field4,field5";
	
	public static final String TEST_GROUPS_STR = "field0,field1,field2,field3,field4,field5";
	
	public static final String TEST_GROUP_BUNDLE_STR2 = 
			"appKey,appVersion,dataType##"
			+ "city,ip,isp,logCity,logProvince##"
			+ "deviceCarrier,deviceHashMac,deviceIMEI,deviceMacAddr,deviceModel,deviceNetwork,deviceOs,deviceOsVersion,deviceResolution,deviceUdid,appChannel##"  
			+ "userName##"
			+ "occurTime,persistedTime##"
			+ "eventId,costTime,logSource##"
			+ "sessionStep";
	
	public static final String TEST_GROUPS_STR2 = 			
			"appKey,appVersion,dataType"
			+ "city,ip,isp,logCity,logProvince"
			+ "deviceCarrier,deviceHashMac,deviceIMEI,deviceMacAddr,deviceModel,deviceNetwork,deviceOs,deviceOsVersion,deviceResolution,deviceUdid,appChannel"  
			+ "userName"
			+ "occurTime,persistedTime"
			+ "eventId,costTime,logSource"
			+ "sessionStep";
}
