package de.webis.copycat_spark.spark;

import java.io.PrintStream;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class SparkIntegrationTestBase extends SharedJavaSparkContext {
	private static PrintStream originalStout;
	
	@BeforeClass
	public static void before() {
		originalStout = System.out;
		System.setOut(System.err);
	}
	
	@AfterClass
	public static void after() {
		System.setOut(originalStout);
	}
	
	@SuppressWarnings("unchecked")
	protected <T> JavaRDD<T> asRDD(T...elements) {
		return jsc().parallelize(Arrays.asList(elements));
	}
}
