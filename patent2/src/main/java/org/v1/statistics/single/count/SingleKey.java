package org.v1.statistics.single.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SingleKey implements WritableComparable<SingleKey>{

	private String field;
	private String value;
	
	public SingleKey(){		
	}
	
	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(field);
		out.writeUTF(value);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.field = in.readUTF();
		this.value = in.readUTF();		
	}

	@Override
	public int compareTo(SingleKey o) {
		int cmp = this.field.compareTo(o.field);
		if (cmp != 0)
			return cmp;
		
		return this.value.compareTo(o.value);
	}

	public void set(String field, String value){
		this.field = field;
		this.value = value;
	}

	@Override
	public String toString() {
		return field + "\t" + value.trim();
	}	
}
