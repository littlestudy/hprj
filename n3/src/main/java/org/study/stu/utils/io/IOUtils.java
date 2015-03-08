package org.study.stu.utils.io;

import java.io.Reader;

import org.study.stu.utils.BaseDataConvert;

public class IOUtils {
	public static JsonToCsvIterator jsonToCsvIterator(Reader reader, BaseDataConvert dataConvert){
		return new JsonToCsvIterator(reader, dataConvert);
	}
	
	public static JsonToCsvTextIterator jsonToCsvTextIterator(Reader reader, BaseDataConvert dataConvert){
		return new JsonToCsvTextIterator(reader, dataConvert);
	}
}
