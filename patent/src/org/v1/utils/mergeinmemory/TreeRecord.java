package org.v1.utils.mergeinmemory;

public class TreeRecord {
	
	private String mKey;
	private String mTree;
		
	private static String SEPARATOR = "##";	
	
	private TreeRecord(String key, String tree){
		this.mKey = key;
		this.mTree = tree;		
	}
	
	public String getKey() {
		return mKey;
	}
	
	public void setKey(String key) {
		this.mKey = key;
	}
	
	public String getTree() {
		return mTree;
	}
	
	public void setTree(String tree) {
		this.mTree = tree;
	}
	
	public String getRoot() {
		return mTree.substring(1, mTree.indexOf(""));
	}
	
	public String append(String trees){
		return trees + mTree;
	}

	@Override
	public String toString() {
		return mKey + "[" + mTree + "]";
	}	
	
	public static void setSeparator(String seperator){
		SEPARATOR = seperator;
	}
	
	/*
	 * str: ..##..## ... ... ##..##AA
	 * 
	 * result
	 * keyStr: ..##..## ... ... ##..
	 * treeStr: (AA)
	 */
	public static TreeRecord fromString(String str){		
		int treeNodeIndex = str.lastIndexOf(SEPARATOR);
		String rootStr = str.substring(treeNodeIndex + SEPARATOR.length());
		String keyStr = str.substring(treeNodeIndex);
		String treeStr = "(" + rootStr + ")";		
			
		return new TreeRecord(keyStr, treeStr);
	}	
	
	/*
	 * trees: (..)[...](..)[...] ... ... (..)[...]
	 */
	public static String generateSubTree(String trees){ 
		return "[" + trees + "]";
	}
	
	/*
	 * keyStr: ..##..##AA
	 * treeStr: (..)[...](..)[...] ... ... (..)[...]
	 *
	 * result
	 * newKey: ..##..
	 * newTree: (AA)[(..)[...](..)[...] ... ... (..)[...]]
	 * 
	 */
	public static TreeRecord generateTreeRecode(String keyStr, String treesStr){
		String subTreesStr = "[" + treesStr + "]";
		
		int newTreeNodeIndex = keyStr.lastIndexOf(SEPARATOR);
		String newKeyStr = null;
		String newTreeStr = null;
		if (newTreeNodeIndex == -1){  // 第一个子段组
			String newRootStr = keyStr;	
			newTreeStr = "(" + newRootStr + ")" + subTreesStr;
		} else {			
			String newRootStr = keyStr.substring(newTreeNodeIndex + SEPARATOR.length());
			keyStr = keyStr.substring(newTreeNodeIndex);
			newTreeStr = "(" + newRootStr + ")" + subTreesStr;
		}		
		
		return new TreeRecord(newKeyStr, newTreeStr);
	}
}
