package de.webis.cikm20_duplicates.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class HashTransformationUtil {

	public static int bytesToInt(byte[] bytes) {
		if(bytes == null || bytes.length != 4) {
			throw new IllegalArgumentException("Input must be not null and 4 bytes");
		}
		
		int ret = 0;
		
		for(int i=0; i<bytes.length; i++) {
			int current = 255 & (bytes[(bytes.length-1) -i]);
			ret = ret ^ (current<<(8*i));
		}
		
		return ret;
	}
	
	public static byte[] intToBytes(int actual, int i) {
		if(i != 4) {
			throw new IllegalArgumentException("I can only convert an int to 4 bytes");
		}
		
		return new byte[] {
			(byte)((actual>>24) & 255),
			(byte)((actual>>16) & 255),
			(byte)((actual>>8) & 255),
			(byte)((actual) & 255)
		};
	}
}
