package de.webis.sigir2021.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TransformJudgedDocumentsToNearDuplicateListsForCC15 {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("sigir2021/url-judgments-from-cw09-or-cw12-to-cc-2015-11");
			
			TransformJudgedDocumentsToNearDuplicateLists.toDuplicatePairs(input)
				.saveAsTextFile("sigir2021/url-judgments-from-cw09-or-cw12-to-cc-2015-11-with-similarity");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("TransformJudgedDocumentsToNearDuplicateListsForCC15");

		return new JavaSparkContext(conf);
	}
}
