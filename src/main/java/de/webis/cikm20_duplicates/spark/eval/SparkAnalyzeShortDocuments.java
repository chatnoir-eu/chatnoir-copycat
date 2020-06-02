package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class SparkAnalyzeShortDocuments {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: new String[] {"cw09-cw12-cc-2015-11", "cc-2017-04"}) {
				JavaRDD<String> input = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-exact-duplicates");
				
				input.flatMap(i -> idsOfZeroHash(i))
					.distinct()
					.repartition(5)
					.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-short-documents");
			}
		}
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static Iterator<String> idsOfZeroHash(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		List<Integer> hash = (List<Integer>) parsed.get("hash");
		
		// this is the "empty-document": as all bits remain to zero and they are masked...
		if(!Arrays.asList(-65536, 65535, 16711935, 16776960).equals(hash)) {
			return Collections.emptyIterator();
		}

		return ids.iterator();
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/eval/analyze_short_documents");

		return new JavaSparkContext(conf);
	}
}
