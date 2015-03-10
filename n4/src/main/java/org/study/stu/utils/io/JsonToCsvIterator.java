package org.study.stu.utils.io;

import java.io.Reader;
import java.util.Iterator;

import org.apache.commons.io.LineIterator;
import org.study.stu.utils.BaseDataConvert;

public class JsonToCsvIterator implements Iterator<String> {
	
	private LineIterator iter;
	private BaseDataConvert dataConvert;
	
    public JsonToCsvIterator(final Reader reader, BaseDataConvert dataConvert) {
    	iter = new LineIterator(reader);
    	this.dataConvert = dataConvert;
    }
    
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public String next() {
		return dataConvert.dataFormat(iter.next());
	}

	@Override
	public void remove() {
		iter.remove();
	}

}
