package org.v1.utils;

import java.io.File;

public class GetProjectPath {
	public static void main(String[] args) {
		String basePath = System.getProperty("user.dir"); // 当前项目的绝对路径
		File file = new File(basePath + "/src/main/resources/data.txt");
		if (file.exists())
			System.out.println(file.getAbsolutePath());
	}
}
