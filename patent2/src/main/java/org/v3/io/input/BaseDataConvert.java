package org.v3.io.input;

import org.v3.common.GroupBundle;

public abstract class BaseDataConvert {
	
	protected GroupBundle groupBundle;
	
	public BaseDataConvert(GroupBundle groupBundle){
		this.groupBundle = groupBundle;
	}
	
	public void setGroupSeparator(String groupSeparator){
		this.groupBundle.setSeparator(groupSeparator);
	}

	public abstract String dataFormat(String originalStr);	
}
