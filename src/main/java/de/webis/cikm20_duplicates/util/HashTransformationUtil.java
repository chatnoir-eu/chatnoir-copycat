package de.webis.cikm20_duplicates.util;

import java.util.Arrays;
import java.util.List;

import lombok.experimental.UtilityClass;

@UtilityClass
public class HashTransformationUtil {

	public static int bytesToInt(byte[] bytes) {
		if (bytes == null || bytes.length != 4) {
			throw new IllegalArgumentException("Input must be not null and 4 bytes");
		}

		int ret = 0;

		for (int i = 0; i < bytes.length; i++) {
			int current = 255 & (bytes[(bytes.length - 1) - i]);
			ret = ret ^ (current << (8 * i));
		}

		return ret;
	}

	public static byte[] intToBytes(int actual, int i) {
		if (i != 4) {
			throw new IllegalArgumentException("I can only convert an int to 4 bytes");
		}

		return new byte[] { (byte) ((actual >> 24) & 255), (byte) ((actual >> 16) & 255), (byte) ((actual >> 8) & 255),
				(byte) ((actual) & 255) };
	}

	static List<Integer> hashToIntegers(byte[] hash, int k) {
		if (k != 3 || hash == null || hash.length != 8) {
			throw new IllegalArgumentException("");
		}

		return Arrays.asList(
				// 0-15. Use 0 bytes at positions 3 and 4 to identify bytes 0-15.
				HashTransformationUtil.bytesToInt(new byte[] { hash[0], hash[1], 0x0, 0x0 }),
				// 16-31. Use 0 bytes at positions 1 and 2 to identify bytes 16-31.
				HashTransformationUtil.bytesToInt(new byte[] { 0x0, 0x0, hash[2], hash[3] }),
				// 32-47. Use 0 bytes at positions 2 and 4 to identify bytes 32-47.
				HashTransformationUtil.bytesToInt(new byte[] { 0x0, hash[4], 0x0, hash[5] }),
				// 48-63. Use 0 bytes at positions 1 and 4 to identify bytes 48-63.
				HashTransformationUtil.bytesToInt(new byte[] { 0x0, hash[6], hash[7], 0x0 }));
	}

	static byte[] integersToHash(List<Integer> ints) {
		failIfIntsAreInvalid(ints);
		byte[] firstInt = intToBytes(ints.get(0), 4);
		byte[] secondInt = intToBytes(ints.get(1), 4);
		byte[] thirdInt = intToBytes(ints.get(2), 4);
		byte[] fourthInt = intToBytes(ints.get(3), 4);
		
		return new byte[] {	
			firstInt[0], firstInt[1],
			secondInt[2], secondInt[3],
			thirdInt[1], thirdInt[3],
			fourthInt[1], fourthInt[2]
		};
	}
	
	static List<Integer> removeIntFromUnderlyingBitArray(List<Integer> ints, Integer remove) {
		failIfIntsAreInvalid(ints);
		int[] pos = positionsOrFail(ints, remove);
		byte[] hash = integersToHash(ints);
		hash = new byte[] {hash[pos[0]], hash[pos[1]], hash[pos[2]], hash[pos[3]], hash[pos[4]], hash[pos[5]]};
		
		return hashToIntegers(new byte[]{
						hash[0], centralBytes((byte) 0x0, hash[1]),
						
						centralBytes(hash[1], (byte) 0x0), hash[2],
						
						hash[3], centralBytes((byte) 0x0, hash[4]),

						centralBytes(hash[4], (byte) 0x0), hash[5]
			}, 3);
	}
	
	private static int[] positionsOrFail(List<Integer> ints, Integer remove) {
		int index = ints.indexOf(remove);
		
		if(index == 0) {
			return new int[] {2, 3, 4, 5, 6, 7};
		} else if(index == 1) {
			return new int[] {0, 1, 4, 5, 6, 7};
		} else if(index == 2) {
			return new int[] {0, 1, 2, 3, 6, 7};
		} else if(index == 3) {
			return new int[] {0, 1, 2, 3, 4, 5};
		} else {
			throw new IllegalArgumentException("");
		}
	}
	
	static byte centralBytes(byte a, byte b) {
		return (byte) (((a & 15) <<4) | ((b>>4) & 15));
	}
	
	private static void failIfIntsAreInvalid(List<Integer> ints) {
		if (ints == null || ints.size() != 4) {
			throw new IllegalArgumentException("");
		}
	}
}
