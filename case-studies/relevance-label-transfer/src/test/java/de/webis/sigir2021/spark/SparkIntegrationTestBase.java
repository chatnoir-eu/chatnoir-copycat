package de.webis.sigir2021.spark;

import java.io.PrintStream;

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
}
