package de.webis.copycat_spark.spark.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRepartitionParts {
	
	public static void main(String[] args) {
		//String inputDir = "cikm2020/canonical-link-graph/cc-2017-04-canonical-urls";
		//String inputDir = "cikm2020/canonical-link-graph/cc-2017-04-sample-0.1";
//		String inputDir = "cikm2020/canonical-link-graph/cc-2015-11-sample-0.1";
//		String inputDir = "cikm2020/document-lengths/cw09-csv.bzip2";
		String inputDir = "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc15-ids-to-remove-ATTENTION-NON-DISTINCT";
		
		try (JavaSparkContext context = context()) {
			for(String suffix: createPartSuffixes()) {
				JavaRDD<String> input = context.textFile(inputDir +"/part*" + suffix);
				input.repartition(5).saveAsTextFile(inputDir + "-delete-me-tmp-"+ suffix);
			}
			
			JavaRDD<String> completeInput = context.textFile(inputDir + "-delete-me-tmp-*/part*");
			completeInput.repartition(1000).saveAsTextFile(inputDir + "-new", BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/repartition-data");
	
		return new JavaSparkContext(conf);
	}

	public static List<String> createPartSuffixes() {
		List<String> ret = new LinkedList<>();
		
		for(int i=0; i < 10; i++) {
			for(int j=0; j<10; j++) {
				ret.add(i + "" + j);
			}
		}
		
		return ret;
	}
}
