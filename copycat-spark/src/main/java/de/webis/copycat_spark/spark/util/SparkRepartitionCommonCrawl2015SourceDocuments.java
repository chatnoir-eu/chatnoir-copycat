package de.webis.copycat_spark.spark.util;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRepartitionCommonCrawl2015SourceDocuments {
	
	private static final String[] CORPORA = new String[] {
			"cc-2015-11-part-0", "cc-2015-11-part-1", "cc-2015-11-part-2",
			"cc-2015-11-part-3", "cc-2015-11-part-4", "cc-2015-11-part-5",
			"cc-2015-11-part-6", "cc-2015-11-part-7", "cc-2015-11-part-8",
			"cc-2015-11-part-9"
	};
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			String corpus = "cc-2015-11";
			
			for(String c: CORPORA) {
				context.textFile("cikm2020/document-fingerprints-final/" + c + "-jsonl")
					.repartition(100)
					.saveAsTextFile("cikm2020/document-fingerprints-final/" + c + "-jsonl-repartioned");
			}
			
			JavaRDD<String> input = context.textFile("cikm2020/document-fingerprints-final/" + corpus +"*-jsonl-repartioned");
			
			input.repartition(10000)
				.saveAsTextFile("cikm2020/document-fingerprints-final/" + corpus +"-jsonl.bzip2", BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/repartition-data");

		return new JavaSparkContext(conf);
	}
}
