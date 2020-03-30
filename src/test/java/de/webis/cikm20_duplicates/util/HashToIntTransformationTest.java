package de.webis.cikm20_duplicates.util;

import org.junit.Assert;
import org.junit.Test;

import static de.webis.cikm20_duplicates.util.HashTransformationUtil.*;

import java.util.Arrays;
import java.util.List;

public class HashToIntTransformationTest {

	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForNullAsInput() {
		bytesToInt(null);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForToLongByteArray() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0, 0x0, 0x0};
		bytesToInt(bytes);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForToShortByteArray() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0};
		bytesToInt(bytes);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForEmptyByteArray() {
		byte[] bytes = new byte[] {};
		bytesToInt(bytes);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForNon4Arrays() {
		intToBytes(0,3);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooSmallK() {
		hashToIntegers(new byte[] {0,0,0,0,0,0,0,0}, 2);
	}
	

	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooLargeK() {
		hashToIntegers(new byte[] {0,0,0,0,0,0,0,0}, 4);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooSmallArray() {
		hashToIntegers(new byte[] {0,0,0,0,0,0,0}, 3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooLargeArray() {
		hashToIntegers(new byte[] {0,0,0,0,0,0,0,0,0}, 3);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForNullInts() {
		integersToHash(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooFewInts() {
		integersToHash(Arrays.asList(0,0,0));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooMuchInts() {
		integersToHash(Arrays.asList(0,0,0,0,0));
	}
	
	@Test
	public void approveHashWithAllZerosAndLength32() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0, 0x0};

		int expected = 0;
		String expectedBinary = "0";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithFourOnesAndLength32() {
		byte[] bytes = new byte[] {0x1, 0x1, 0x1, 0x1};

		int expected = 16843009;
		String expectedBinary = "1000000010000000100000001";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}

	@Test
	public void approveHashWithFourDifferentBytesAndLength32() {
		byte[] bytes = new byte[] {0x1, 0x2, 0x3, 0x4};

		int expected = 16909060;
		String expectedBinary = "1000000100000001100000100";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithLargestBytesAndLength32() {
		byte[] bytes = new byte[] {0x7f, 0x7f, 0x7f, 0x7f};

		int expected = 2139062143;
		String expectedBinary = "1111111011111110111111101111111";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithSmallestBytesAndLength32() {
		byte[] bytes = new byte[] {-0x80, -0x80, -0x80, -0x80};

		int expected = (int) (-2147483648 + 8421504);
		String expectedBinary = "10000000100000001000000010000000";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithOnlyMinusOneBytesAndLength32() {
		byte[] bytes = new byte[] {-0x01, -0x01, -0x01, -0x01};

		int expected = -1;
		String expectedBinary = "11111111111111111111111111111111";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithSomeMinusOneBytesAndLength32() {
		byte[] bytes = new byte[] {-0x01, 0x0, 0x01, -0x01};

		int expected = (int) (-2147483648 + 2130706943);
		String expectedBinary = "11111111000000000000000111111111";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveHashWithOneMinusAndSomePlusBytesAndLength32() {
		byte[] bytes = new byte[] {0x1, 0x2, 0x3, -0x01};

		int expected = 16909311;
		String expectedBinary = "1000000100000001111111111";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
	
	@Test
	public void approveTransformationForAllZeroBytesAndLength64() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
		List<Integer> ints = Arrays.asList(0, 0, 0, 0);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveTransformationForAscendingBytesAndLength64() {
		byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
		List<Integer> ints = Arrays.asList(16908288, 772, 327686, 460800);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveTransformationForDescendingBytesAndLength64() {
		byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x04, 0x02, 0x01};
		List<Integer> ints = Arrays.asList(134676480, 1541, 262148, 131328);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveTransformationForAllMinusOneAndLength64() {
		byte[] bytes = new byte[] {-0x01, -0x01, -0x01, -0x01, -0x01, -0x01, -0x01, -0x01};
		List<Integer> ints = Arrays.asList(-65536, 65535, 16711935, 16776960);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveTransformationForAlternatingMinusOneMaxAndLength64() {
		byte[] bytes = new byte[] {-0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f};
		List<Integer> ints = Arrays.asList(-8454144, 65407, 16711807, 16744192);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveTransformationForAlternatingMaxAndMinusOneAndLength64() {
		byte[] bytes = new byte[] {0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01};
		List<Integer> ints = Arrays.asList(2147418112, 32767, 8323327, 8388352);
		
		Assert.assertEquals(ints, hashToIntegers(bytes, 3));
		Assert.assertArrayEquals(bytes, integersToHash(ints));
	}
	
	@Test
	public void approveWithRandomIntsFor64Bits() {
		for(int i=0; i<1000; i++) {
			byte[] bytes = new byte[] {rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand()};
			String message = "[" + ((int) bytes[0]) + "," + ((int) bytes[1]) + "," + ((int) bytes[2]) + ","
							+((int) bytes[3]) + "," +((int) bytes[4]) + "," + ((int) bytes[5]) + ","
							+((int) bytes[6]) + "," +((int) bytes[7]) + "]";
		
			Assert.assertArrayEquals("Look at " + message, bytes, integersToHash(hashToIntegers(bytes, 3)));
			Assert.assertEquals("Look at " + message, hashToIntegers(bytes, 3), hashToIntegers(integersToHash(hashToIntegers(bytes, 3)), 3));
		}
	}
	
	private byte rand() {
		return (byte) (int)((Math.random()*255) -128);
	}
}
