package org.study.stu.common.dict;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RDictionaryBundle {
	
	/*
	 * field和code是1对1关系
	 */	
	private class Dictionary{
		private HashMap<Integer, String> map;
		
		public String search(int code){
			return map.get(code);
		}
		
		public void fromByteBuffer(ByteBuffer bb) throws IOException{
			this.map = null;
			byte[] mapBytes = new byte[bb.remaining()];
			bb.get(mapBytes, 0, bb.remaining());
			ByteArrayInputStream bais = new ByteArrayInputStream(mapBytes);
			DataInput dataInput = new DataInputStream(bais);
			int mapSize = dataInput.readInt();
			//System.out.println("------mapSize: " + mapSize);
			map = new HashMap<Integer, String>(mapSize);
			for (int i = 0; i < mapSize; i++){
				String val = dataInput.readUTF();				
				map.put((int) dataInput.readShort(), val);
			}
		}
		
		public int dictSize(){
			return map.size();
		}
		
		public void showDictionary(){ 
			Iterator<Entry<Integer, String>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<Integer, String> entry = (Map.Entry<Integer, String>) iter.next();
				System.out.println("Code: " + entry.getKey() + ", Field: " + entry.getValue());
			}
		} 
	}
	
	private List<Dictionary> bundle;
	
	public RDictionaryBundle(int dictAmount){
		bundle = new ArrayList<RDictionaryBundle.Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public String search(int dictNum, int code){
		return bundle.get(dictNum).search(code); 
	}
	
	public void setDictFromByteBuffer(int dictNum, ByteBuffer bb) throws IOException{
		bundle.get(dictNum).fromByteBuffer(bb);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}			
	}
	
	public int getDictionarySize(int dictNum){
		return bundle.get(dictNum).dictSize();
	}
}
