package org.v2.ioformat.input;

import org.v2.common.GroupBundle;

public abstract class BaseDataConvert {
	
	private GroupBundle groupBundle;
	
	public BaseDataConvert(GroupBundle groupBundle){
		this.groupBundle = groupBundle;
	}
	
	public void setGroupSeparator(String groupSeparator){
		this.groupBundle.setSeparator(groupSeparator);
	}

	public abstract String dataFormat(String originalStr);
	
	
}
