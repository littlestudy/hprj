package org.study.stu.utils;

import org.study.stu.common.GroupBundle;

public abstract class BaseDataConvert {
	
	protected GroupBundle groupBundle;
	
	public BaseDataConvert(GroupBundle groupBundle){
		this.groupBundle = groupBundle;
	}
	
	public void setGroupSeparator(String groupSeparator){
		this.groupBundle.setGroupSeparator(groupSeparator);
	}

	public abstract String dataFormat(String originalStr);	
}
