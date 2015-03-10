package org.study.stu.utils.io;

import java.io.Reader;
import java.util.Iterator;

import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.study.stu.utils.BaseDataConvert;

public class JsonToCsvTextIterator implements Iterator<Text> {
	
	private LineIterator iter;
	private BaseDataConvert dataConvert;
	
    public JsonToCsvTextIterator(final Reader reader, BaseDataConvert dataConvert) {
    	iter = new LineIterator(reader);
    	this.dataConvert = dataConvert;
    }
    
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Text next() {
		return new Text(dataConvert.dataFormat(iter.next()));
	}

	@Override
	public void remove() {
		iter.remove();
	}

}
