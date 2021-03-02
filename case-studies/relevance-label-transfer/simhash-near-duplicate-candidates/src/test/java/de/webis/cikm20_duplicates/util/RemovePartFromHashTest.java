package de.webis.cikm20_duplicates.util;

import org.junit.Assert;
import org.junit.Test;

import static de.webis.cikm20_duplicates.util.HashTransformationUtil.*;
import static de.webis.cikm20_duplicates.util.HashToIntTransformationTest.*;

import java.util.Arrays;
import java.util.List;

public class RemovePartFromHashTest {
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooSmallList() {
		removeIntFromUnderlyingBitArray(Arrays.asList(1, 2, 3), 4);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForTooLargeList() {
		removeIntFromUnderlyingBitArray(Arrays.asList(1, 2, 3, 4, 5), 6);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForNonExistingRemovalInt() {
		removeIntFromUnderlyingBitArray(Arrays.asList(1, 2, 3, 4), 5);
	}
	
	@Test
	public void combineZeroByteAndZeroByte() {
		byte a = 0x0;
		byte b = 0x0;
		String expected = "00000000";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combineMinusOneByteAndZeroByte() {
		byte a = -1;
		byte b = 0x0;
		String expected = "11110000";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combineZeroByteAndMinusOneByte() {
		byte a = 0x0;
		byte b = -0x1;
		String expected = "00001111";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combineMinusOneByteAndMinusOneByte() {
		byte a = -0x1;
		byte b = -0x1;
		String expected = "11111111";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combineMinusOneByteAndOneByte() {
		byte a = -0x1;
		byte b = 0x1;
		String expected = "11110000";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}

	@Test
	public void combineOneByteAndMinusOneByte() {
		byte a = 0x1;
		byte b = -0x1;
		String expected = "00011111";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combine16ByteAndMinusOneByte() {
		byte a = 0x10;
		byte b = -0x1;
		String expected = "00001111";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void combineMinusOneByteAnd16Byte() {
		byte a = -0x1;
		byte b = 0x10;
		String expected = "11110001";
		byte actual = centralBytes(a,b);
		
		Assert.assertEquals(expected, binary(actual));
	}
	
	@Test
	public void approveTransformationForAllMinusOneAndLength64() {
		List<Integer> input = Arrays.asList(-65536, 65535, 16711935, 16776960);
		
		for(int toRemove: input) {
			//byte[] bytes = new byte[] {-0x01, -0x01, -0x01, -0x01, -0x01, -0x01, -0x01, -0x01};
			String expectedBeforeReduction = "11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111";
			String expectedAfterReduction =  "11111111 00001111 11110000 11111111 11111111 00001111 11110000 11111111";
			
			Assert.assertEquals(expectedBeforeReduction, binary(input));

			List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, toRemove);

			Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));	
		}
	}
	
	@Test
	public void approveTransformationForAllZeroBytesAndLength64() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
		String expectedAfterReduction = "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000";
		List<Integer> input = Arrays.asList(0, 0, 0, 0);
		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 0);
		
		Assert.assertEquals(input, sizeReduced);
		Assert.assertArrayEquals(bytes, integersToHash(sizeReduced));
		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
	@Test
	public void approveTransformationForAscendingBytesAndLength64RemovingTheFirstInteger() {
		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
		
		//byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
		String expectedAfterReduction =  "00000011 00000000 01000000 00000101 00000110 00000000 01110000 00001000";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 16908288);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
	@Test
	public void approveTransformationForAscendingBytesAndLength64RemovingTheSecondInteger() {
		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
		
		//byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
		String expectedAfterReduction =  "00000001 00000000 00100000 00000101 00000110 00000000 01110000 00001000";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 772);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
	@Test
	public void approveTransformationForAscendingBytesAndLength64RemovingTheThirdInteger() {
		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
		
		//byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
		String expectedAfterReduction =  "00000001 00000000 00100000 00000011 00000100 00000000 01110000 00001000";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 327686);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
	@Test
	public void approveTransformationForAscendingBytesAndLength64RemovingTheFourthInteger() {
		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
		
		//byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
		String expectedAfterReduction =  "00000001 00000000 00100000 00000011 00000100 00000000 01010000 00000110";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 460800);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
	@Test
	public void approveTransformationForDescendingBytesAndLength64RemovingTheFirstInteger() {	
		List<Integer> input = Arrays.asList(134676480, 1541, 262148, 131328);
		
		//byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x04, 0x02, 0x01};
		String expectedBeforeReduction = "00001000 00000111 00000110 00000101 00000100 00000100 00000010 00000001";
		String expectedAfterReduction =  "00000110 00000000 01010000 00000100 00000100 00000000 00100000 00000001";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 134676480);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}

	@Test
	public void approveTransformationForDescendingBytesAndLength64RemovingTheSecondInteger() {	
		List<Integer> input = Arrays.asList(134676480, 1541, 262148, 131328);
		
		//byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x04, 0x02, 0x01};
		String expectedBeforeReduction = "00001000 00000111 00000110 00000101 00000100 00000100 00000010 00000001";
		String expectedAfterReduction =  "00001000 00000000 01110000 00000100 00000100 00000000 00100000 00000001";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 1541);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}

	@Test
	public void approveTransformationForDescendingBytesAndLength64RemovingTheThirdInteger() {	
		List<Integer> input = Arrays.asList(134676480, 1541, 262148, 131328);
		
		//byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x04, 0x02, 0x01};
		String expectedBeforeReduction = "00001000 00000111 00000110 00000101 00000100 00000100 00000010 00000001";
		String expectedAfterReduction =  "00001000 00000000 01110000 00000110 00000101 00000000 00100000 00000001";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 262148);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}

	@Test
	public void approveTransformationForDescendingBytesAndLength64RemovingThefourthInteger() {	
		List<Integer> input = Arrays.asList(134676480, 1541, 262148, 131328);
		
		//byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x04, 0x02, 0x01};
		String expectedBeforeReduction = "00001000 00000111 00000110 00000101 00000100 00000100 00000010 00000001";
		String expectedAfterReduction =  "00001000 00000000 01110000 00000110 00000101 00000000 01000000 00000100";
		
		Assert.assertEquals(expectedBeforeReduction, binary(input));

		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 131328);

		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}

	@Test
	public void approveTransformationForAlternatingMinusOneMaxAndLength64() {
		List<Integer> input = Arrays.asList(-8454144, 65407, 16711807, 16744192);
		
		for(int toRemove: input) {
		
			//byte[] bytes = new byte[] {-0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f};
			String expectedBeforeReduction = "11111111 01111111 11111111 01111111 11111111 01111111 11111111 01111111";
			String expectedAfterReduction =  "11111111 00000111 11110000 11111111 01111111 00001111 11110000 01111111";
			
			Assert.assertEquals(expectedBeforeReduction, binary(input));
	
			List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, toRemove);
	
			Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
		}
	}
	
	@Test
	public void approveTransformationForAlternatingMaxAndMinusOneAndLength64() {
		List<Integer> input = Arrays.asList(2147418112, 32767, 8323327, 8388352);
		
		for(int toRemove: input) {
			//byte[] bytes = new byte[] {0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01, 0x7f, -0x01};
			String expectedBeforeReduction = "01111111 11111111 01111111 11111111 01111111 11111111 01111111 11111111";
			String expectedAfterReduction =  "01111111 00001111 11110000 01111111 11111111 00000111 11110000 11111111";
			
			Assert.assertEquals(expectedBeforeReduction, binary(input));
	
			List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, toRemove);
	
			Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
		}
	}
	
	@Test
	public void approveWithRandomIntsFor64Bits() {
		for(int i=0; i<5000; i++) {
			byte[] bytes = new byte[] {rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand()};
			String message = "[" + ((int) bytes[0]) + "," + ((int) bytes[1]) + "," + ((int) bytes[2]) + ","
							+((int) bytes[3]) + "," +((int) bytes[4]) + "," + ((int) bytes[5]) + ","
							+((int) bytes[6]) + "," +((int) bytes[7]) + "]";
			List<Integer> input = hashToIntegers(bytes, 3);
			String expectedBeforeReduction = binary(input);
			
			for(int toRemove : input) {
				Assert.assertEquals("Look at " + message, expectedBeforeReduction, binary(input));
				
				String expectedAfterReduction =  expectedBinaryAfterRemoval(input, toRemove); 
				List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, toRemove);
				
				Assert.assertEquals("Look at " + message, expectedAfterReduction, binary(sizeReduced));
//				Assert.assertEquals("Look at inverse for " + message, input, reverse());
			}
		}
	}
	
	private static String expectedBinaryAfterRemoval(List<Integer> input, int toRemove) {
		String expectedBeforeReduction = binary(input);
		int pos = input.indexOf(toRemove);
		String[] bits = expectedBeforeReduction.split("\\s+");
		
		if (pos == 0) {
			bits = new String[] { bits[2], bits[3], bits[4], bits[5], bits[6], bits[7]};
		} else if (pos == 1) {
			bits = new String[] { bits[0], bits[1], bits[4], bits[5], bits[6], bits[7]};
		} else if (pos == 2) {
			bits = new String[] { bits[0], bits[1], bits[2], bits[3], bits[6], bits[7]};
		} else if (pos == 3) {
			bits = new String[] { bits[0], bits[1], bits[2], bits[3], bits[4], bits[5]};
		} else {
			throw new RuntimeException("");
		}
		
		return bits[0] + " " + left(bits[1]) + " " + right(bits[1]) + " " + bits[2] + " " + bits[3] + " " + left(bits[4]) + " " + right(bits[4]) + " " + bits[5];
	}
	
	private static String left(String s) {
		return "0000" + s.substring(0, 4);
	}
	
	private static String right(String s) {
		return s.substring(4, 8) + "0000";
	}
	
	static String binary(List<Integer> ints) {
		String ret = "";
		byte[] bytes = integersToHash(ints);
		
		for(byte b: bytes) {
			ret += binary(b) + " ";
		}
		
		return ret.trim();
	}
	
	private static String binary(byte b) {
		String ret = "";
		
		for(int i=0; i<8;i++) {
			ret =  byteAtPos(b,i) + ret;
		}
		
		return ret;
	}

	private static String byteAtPos(byte b, int pos) {
		return (b>>pos & 0x1) > 0 ? "1" : "0";
	}
}
