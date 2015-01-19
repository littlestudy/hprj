package org.v1.utils.mergeinmemory;

import java.util.List;

public class TreeRecord {
	
	private String mKey;
	private String mRoot;		// mRoot: (...)
	private String mSubTrees;	// mSubTrees: [(.)[..](.)[..] ... ... (.)[..]]
								// Tree: mRoot + mSubTrees
	
	public TreeRecord(String key, String root, String subTrees) {
		super();
		this.mKey = key;
		this.mRoot = root;
		this.mSubTrees = subTrees;
	}

	public String getKey() {
		return mKey;
	}
	
	public void setKey(String key) {
		this.mKey = key;
	}	

	public String getRoot() {
		return mRoot.substring(1, mRoot.lastIndexOf(")"));
	}

	public void setRoot(String root) {
		this.mRoot = "(" + root + ")";
	}

	public String getSubTrees() {
		return mSubTrees.substring(1, mSubTrees.lastIndexOf("]"));
	}

	public void setSubTrees(String subTrees) {
		this.mSubTrees = "[" + subTrees + "]";
	}
	
	public String getTree(){
		return mRoot + mSubTrees;
	}

	@Override
	public String toString() {
		return mKey + mRoot + mSubTrees;
	}	
	
	public static TreeRecord fromOriginalString(String str){
		return fromOriginalString(str, "##");
	}
	/*
	 * str: ..##..## ... ... ##..##AA
	 * 
	 * result
	 * mKey: 		..##..## ... ... ##..
	 * mRoot: 		(AA)
	 * mSubTrees: 	""
	 */
	public static TreeRecord fromOriginalString(String str, String separator){
		int lastIndex = str.lastIndexOf(separator);
		String key = str.substring(0, lastIndex);
		String root = str.substring(lastIndex + separator.length());
		return new TreeRecord(key, root, "");
	}	
	
	public static TreeRecord generateTreeRecodeFromTreeList(
		String keyStr, List<TreeRecord> treeList){
		
		return generateTreeRecodeFromTreeList(keyStr, treeList, "##");
	}
	
	/*
	 * keyStr: 			..##..##AA
	 * treeStr: 		(..)[...](..)[...] ... ... (..)[...]
	 *
	 * result
	 * newKey: 			..##..
	 * newRootStr: 		AA
	 * newSubTreesStr: 	(..)[...](..)[...] ... ... (..)[...]
	 * 
	 */
	public static TreeRecord generateTreeRecodeFromTreeList(
			String keyStr, List<TreeRecord> treeList, String separator){
		
		StringBuilder sb = new StringBuilder();
		for (TreeRecord treeRecord : treeList){
			sb.append(treeRecord.getTree());
		}
		String newSubTreesStr = sb.toString();
		sb = null;
		
		int newTreeNodeIndex = keyStr.lastIndexOf(separator); 
		String newKeyStr = null;
		String newRootStr = null;		
		if (newTreeNodeIndex == -1){  // 第一个字段组
			newRootStr = keyStr;	
		} else {			
			newRootStr = keyStr.substring(newTreeNodeIndex + separator.length());
			keyStr = keyStr.substring(newTreeNodeIndex);			
		}		
		
		return new TreeRecord(newKeyStr, newRootStr, newSubTreesStr);
	}
}
