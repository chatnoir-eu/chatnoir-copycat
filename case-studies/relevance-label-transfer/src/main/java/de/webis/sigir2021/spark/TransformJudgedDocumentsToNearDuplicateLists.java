package de.webis.sigir2021.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;

public class TransformJudgedDocumentsToNearDuplicateLists {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("sigir2021/url-judgments-from-cw09-to-cw12");
			
			toDuplicatePairs(input)
				.saveAsTextFile("sigir2021/url-judgments-from-cw09-to-cw12-with-similarity");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("JudgedDocumentsInDocumentRepresentations");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> toDuplicatePairs(JavaRDD<String> input) {
		return input.flatMap(i -> toDuplicatePairs(i));
	}
	
	private static Iterator<String> toDuplicatePairs(String src) {
		List<String> ret = new ArrayList<>();
		JSONObject parsedJson = new JSONObject(src);
		String sourceId = parsedJson.getString("sourceId");
		JSONArray targetIds = parsedJson.getJSONArray("targetIds");

		for(int i=0; i<targetIds.length(); i++) {
			ret.add("{\"firstId\":\"" + sourceId + "\",\"secondId\":\"" + targetIds.getString(i) + "\",\"hemmingDistance\":-1}");
		}
		
		return ret.iterator();
	}
}
