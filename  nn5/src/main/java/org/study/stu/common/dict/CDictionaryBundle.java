package org.study.stu.common.dict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CDictionaryBundle {
	
	/*
	 * key和code是1对1关系
	 */	
	private class Dictionary{
		private int codeTop;
		private HashMap<String, Integer> map;
		
		public Dictionary(){
			codeTop = 0;
			map = new HashMap<String, Integer>();
		}
		
		public Integer find(String key){
			Integer code = map.get(key);
			if (code == null) {
				map.put(key, codeTop);
				code = codeTop;
				codeTop++;
			}
			return code;
		}
		
		public Integer search(String key){
			Integer code = map.get(key);
			if (code == null)
				return -1;
			
			return code;
		}
		
		public int dictSize(){
			return map.size();
		}
		
		public void showDictionary(){
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				System.out.println("Item: " + entry.getKey() + ", Code: " + entry.getValue());
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				sb.append(",").append(entry.getKey()).append(",").append(String.valueOf(entry.getValue()));	
			}
			
			return sb.substring(1);
		}	
		
		public void write(DataOutput out) throws IOException{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput dataOutput = new DataOutputStream(baos);
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			dataOutput.writeInt(map.size());
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				dataOutput.writeUTF(entry.getKey());
				dataOutput.writeShort(entry.getValue());
			}
			dataOutput = null;
			baos.close();
			byte[] data = baos.toByteArray();
			out.write(data, 0, data.length);		
		}
		
		public void fromByteBuffer(ByteBuffer bb) throws IOException{
			this.map = null;			
			byte[] mapBytes = new byte[bb.remaining()];
			bb.get(mapBytes, 0, bb.remaining());
			ByteArrayInputStream bais = new ByteArrayInputStream(mapBytes);
			DataInput dataInput = new DataInputStream(bais);
			int mapSize = dataInput.readInt();
			map = new HashMap<String, Integer>(mapSize);
			for (int i = 0; i < mapSize; i++){
				map.put(dataInput.readUTF(), (int) dataInput.readShort());
			}
		}
	}
	
	private List<Dictionary> bundle;
	
	public CDictionaryBundle(int dictAmount){
		bundle = new ArrayList<CDictionaryBundle.Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public Integer find(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.find(key);
	}
	
	public Integer search(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.search(key);
	}
	
	public int getDictionaryAmount(){
		return bundle.size();
	}
	
	public String dictionaryToString(int dicNum){
		return bundle.get(dicNum).toString();
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}			
	}
	
	public Map<String, Integer> getRawDictionary(int i){
		return bundle.get(i).map;
	}
	
	public void writeAllDicts(DataOutput out) throws IOException{
		out.writeInt(bundle.size());		
		for (int i = 0; i < bundle.size(); i++)
			bundle.get(i).write(out);
	}
	
	public void setDictFromByteBuffer(int dictNum, ByteBuffer bb) throws IOException{
		bundle.get(dictNum).fromByteBuffer(bb);
	}
	
	public int getDictionarySize(int dictNum){
		return bundle.get(dictNum).dictSize();
	}
}
