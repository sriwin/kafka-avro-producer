package com.sriwin.kafka.utils;

import java.io.File;

public class DirUtil {
	public static final String FILE_SEPARATOR = File.separator;
	public static final String CONFIG_DIR = "config";

	public static String getConfigDir() {
		return getUserDir() + CONFIG_DIR + FILE_SEPARATOR;
	}

	public static String getUserDir() {
		String userDir = System.getProperty("user.dir") + FILE_SEPARATOR;
		return userDir;
	}
}