package com.scistor.process.utils.params;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class SystemConfig {

	private static final String BUNDLE_NAME = "system";
	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	private SystemConfig() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key).trim();
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}

}
