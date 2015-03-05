package org.study.stu.utils;

import org.study.stu.common.GroupBundle;

public abstract class DataConvertBase {
	
	protected GroupBundle groupBundle;
	
	public DataConvertBase(GroupBundle groupBundle){
		this.groupBundle = groupBundle;
	}
	
	public void setGroupSeparator(String groupSeparator){
		this.groupBundle.setGroupSeparator(groupSeparator);
	}

	public abstract String dataFormat(String originalStr);	
}
