package de.webis.cikm20_duplicates.util;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TakeRandomTest {
	@Test
	public void take10FromEmptyIterable() {
		Iterable<String> input = Collections.emptyList();
		List<String> expected = Collections.emptyList();
		
		List<String> actual = takeRandomElementsAndSort(10, input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void take10FromNonEmptyIterable() {
		Iterable<String> input = Arrays.asList("a", "b", "a", "b");
		List<String> expected = Arrays.asList("a", "a", "b", "b");
		List<String> actual = takeRandomElementsAndSort(10, input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void take1FromNonEmptyIterable() {
		Iterable<String> input = Arrays.asList("a", "b", "c", "d", "e", "f");
		List<String> expected = Arrays.asList("a");
		
		for(int i=0; i<10000; i++) {
			Random r = mockRandom(1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d);
			List<String> actual = takeRandomElementsAndSort(1, input, r);
			
			Assert.assertEquals(expected, actual);	
		}
	}
	
	@Test
	public void take2FromNonEmptyIterable() {
		Iterable<String> input = Arrays.asList("a", "b", "c", "d", "e", "f");
		List<String> expected = Arrays.asList("c", "f");
		
		for(int i=0; i<10000; i++) {
			Random r = mockRandom(0d, 0d, 1d, 0d, 0d, 1d, 0d, 0d);
			List<String> actual = takeRandomElementsAndSort(2, input, r);
			
			Assert.assertEquals(expected, actual);	
		}
	}
	
	@Test
	public void takeFirstTenFromEmptyIterable() {
		Iterable<String> input = Collections.emptyList();
		List<String> expected = Collections.emptyList();
		
		List<String> actual = TakeRandom.takeFirst(10, input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void takeFirstThreeFromNonEmptyIterable() {
		Iterable<String> input = Arrays.asList("a", "b", "a", "b");
		List<String> expected = Arrays.asList("a", "b", "a");
		List<String> actual = TakeRandom.takeFirst(3, input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void takeFirstHundredFromNonEmptyIterable() {
		Iterable<String> input = Arrays.asList("a", "b", "c", "d", "e", "f");
		List<String> expected = Arrays.asList("a", "b", "c", "d", "e", "f");
		
		List<String> actual = TakeRandom.takeFirst(100, input);
		Assert.assertEquals(expected, actual);	
	}
	
	private static Random mockRandom(Double...vals) {
		Iterator<Double> tmp = Arrays.asList(vals).iterator(); 
		Random r = Mockito.mock(Random.class);
		
		Mockito.when(r.nextDouble()).thenAnswer(new Answer<Double>() {
			@Override
			public Double answer(InvocationOnMock invocation) throws Throwable {
				return tmp.next();
			}
		});
		
		return r;
	}
	
	private static <T extends Comparable<? super T>> List<T> takeRandomElementsAndSort(int k, Iterable<T> input) {
		return takeRandomElementsAndSort(k, input, new Random());
	}
	
	private static <T extends Comparable<? super T>> List<T> takeRandomElementsAndSort(int k, Iterable<T> input, Random random) {
		return TakeRandom.takeRandomElements(k, input, random).stream()
				.sorted()
				.collect(Collectors.toList());
	}
}
