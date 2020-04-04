package de.webis.cikm20_duplicates.util;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static de.webis.cikm20_duplicates.util.HashTransformationUtil.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
	public void approveTransformationForAllZeroBytesAndLength64() {
		byte[] bytes = new byte[] {0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
		String expectedAfterReduction = "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000";
		List<Integer> input = Arrays.asList(0, 0, 0, 0);
		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 0);
		
		Assert.assertEquals(input, sizeReduced);
		Assert.assertArrayEquals(bytes, integersToHash(sizeReduced));
		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
	}
	
//	@Test
//	public void approveTransformationForAscendingBytesAndLength64RemovingTheFirstInteger() {
//		byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
//		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
//		
//		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
//		String expectedAfterReduction =  "00000011 00000000 00000100 00000101 00000110 00000000 00000111 00001000";
//		
//		Assert.assertEquals(expectedBeforeReduction, binary(input));
//
//		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 16908288);
//
//		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
//	}
//	
//	@Test
//	public void approveTransformationForAscendingBytesAndLength64RemovingTheSecondInteger() {
//		byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
//		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
//		
//		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
//		String expectedAfterReduction =  "00000001 00000000 00000010 00000101 00000110 00000000 00000111 00001000";
//		
//		Assert.assertEquals(expectedBeforeReduction, binary(input));
//
//		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 772);
//
//		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
//	}
//
//	@Test
//	public void approveTransformationForAscendingBytesAndLength64RemovingTheThirdInteger() {
//		byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
//		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
//		
//		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
//		String expectedAfterReduction =  "00000001 00000000 00000010 00000011 00000100 00000000 00000111 00001000";
//		
//		Assert.assertEquals(expectedBeforeReduction, binary(input));
//
//		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 327686);
//
//		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
//	}
//
//	@Test
//	public void approveTransformationForAscendingBytesAndLength64RemovingTheFourthInteger() {
//		byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
//		List<Integer> input = Arrays.asList(16908288, 772, 327686, 460800);
//		
//		String expectedBeforeReduction = "00000001 00000010 00000011 00000100 00000101 00000110 00000111 00001000";
//		String expectedAfterReduction =  "00000001 00000000 00000010 00000011 00000100 00000000 00000101 00000110";
//		
//		Assert.assertEquals(expectedBeforeReduction, binary(input));
//
//		List<Integer> sizeReduced = removeIntFromUnderlyingBitArray(input, 460800);
//
//		Assert.assertEquals(expectedAfterReduction, binary(sizeReduced));
//	}

	private static String binary(List<Integer> ints) {
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
