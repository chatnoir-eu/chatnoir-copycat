package de.webis.cikm20_duplicates.spark.util;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments;

public class SparkRepartitionCommonCrawl2017SourceDocuments {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			String corpus = "cc-2017-04";
			
			for(String c: SparkCreateSourceDocuments.cc17Collections()) {
				context.textFile("cikm2020/document-fingerprints-final/" + c + "-jsonl")
					.repartition(50)
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
