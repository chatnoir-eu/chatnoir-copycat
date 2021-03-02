package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.Iterators;

import de.webis.cikm20_duplicates.app.SampleNearDuplicates.NearDuplicate;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction;

public class FilterExactDuplicatesBetweenCorpora {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/exact-duplicates");
			
			exactDuplicatesBetweenCorporaWithJudgments(input)
				.saveAsTextFile("sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-exact-duplicates-between-corpora-for-relevance-transfer", BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": FilterExactDuplicatesBetweenCorpora");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> exactDuplicatesBetweenCorporaWithJudgments(JavaRDD<String> exactDuplicates) {
		return exactDuplicates.flatMap(i -> exactDuplicatePairsBetweenCorporaWithJudgments(i));
	}

	private static Iterator<String> exactDuplicatePairsBetweenCorporaWithJudgments(String exactDuplicates) {
		JSONObject parsed = new JSONObject(exactDuplicates);
		JSONArray equivalentDocuments = parsed.getJSONArray("equivalentDocuments");
		List<String> cw09Docs = new ArrayList<>();
		List<String> cw12Docs = new ArrayList<>();
		List<String> cc15Docs = new ArrayList<>();
		
		for(int i=0; i< equivalentDocuments.length(); i++) {
			String documentId = equivalentDocuments.getString(i);
			
			if (documentId.startsWith("clueweb09")) {
				cw09Docs.add(documentId);
			} else if (documentId.startsWith("clueweb12")) {
				cw12Docs.add(documentId);
			} else {
				cc15Docs.add(documentId);
			}
		}
		
		List<String> cw09DocsWithJudgment = cw09Docs.stream().filter(i -> hasSomeJudgment(i)).collect(Collectors.toList());
		
		List<String> cw09TargetDocuments = new ArrayList<>(cw12Docs);
		cw09TargetDocuments.addAll(cc15Docs);
		
		List<String> cw12DocsWithJudgment = cw12Docs.stream().filter(i -> hasSomeJudgment(i)).collect(Collectors.toList());
		List<String> cw12TargetDocuments = new ArrayList<>(cc15Docs);
		
		Iterator<String> cw09Pairs = allPairsFromSourceToTarget(cw09DocsWithJudgment, cw09TargetDocuments);
		Iterator<String> cw12Pairs = allPairsFromSourceToTarget(cw12DocsWithJudgment, cw12TargetDocuments);

		return Iterators.concat(cw09Pairs, cw12Pairs);
	}
	
	private static boolean hasSomeJudgment(String i) {
		return !SparkRelevanceTransferDataConstruction.possibleRelevanceTransfersFromTo(i, "my-dummy-id", 0).isEmpty();
	}

	private static Iterator<String> allPairsFromSourceToTarget(List<String> source, List<String> target) {
		if(source == null || target == null) {
			return Collections.emptyIterator();
		}
		
		return source.stream().flatMap(i -> allPairsFromSourceToTarget(i, target)).iterator();
	}
	
	private static Stream<String> allPairsFromSourceToTarget(String source, List<String> target) {
		if(source == null || target == null) {
			return new ArrayList<String>().stream();
		}
		
		return target.stream().map(i -> {
			NearDuplicate nd = new NearDuplicate();
			nd.setHemmingDistance(0);
			nd.setFirstId(source);
			nd.setSecondId(i);
			
			return nd.toString();
		});
	}
}
