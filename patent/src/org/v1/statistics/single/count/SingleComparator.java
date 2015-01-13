package org.v1.statistics.single.count;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SingleComparator extends WritableComparator{
	
	protected SingleComparator(){		
		super(SingleKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		SingleKey f1 = (SingleKey)w1;
		SingleKey f2 = (SingleKey)w2;
		
		int cmp = f1.getField().compareTo(f2.getField());
		if (cmp != 0)
			return cmp;
		return f1.getValue().compareTo(f2.getValue());
	}
}
