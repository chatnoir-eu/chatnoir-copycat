package de.webis.cikm20_duplicates.util;

import org.junit.Assert;
import org.junit.Test;

import static de.webis.cikm20_duplicates.util.HashTransformationUtil.*;

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
	
	@Test
	public void approveHashWithAllZerosAndLength64() {
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
	public void approveHashWithFourOnesAndLength64() {
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
	public void approveHashWithFourDifferentBytesAndLength64() {
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
	public void approveHashWithLargestBytesAndLength64() {
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
	public void approveHashWithSmallestBytesAndLength64() {
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
	public void approveHashWithOnlyMinusOneBytesAndLength64() {
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
	public void approveHashWithSomeMinusOneBytesAndLength64() {
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
	public void approveHashWithOneMinusAndSomePlusBytesAndLength64() {
		byte[] bytes = new byte[] {0x1, 0x2, 0x3, -0x01};

		int expected = 16909311;
		String expectedBinary = "1000000100000001111111111";
		int actual = bytesToInt(bytes);
		String actualBinary = Integer.toBinaryString(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedBinary, actualBinary);
		Assert.assertArrayEquals(bytes, intToBytes(actual, 4));
	}
}
