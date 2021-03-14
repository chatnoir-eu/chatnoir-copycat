package de.webis.cikm20_duplicates.spark.eval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkSampleS3EdgesPerBin {
	
	private static final String DIR = "cikm2020/canonical-link-graph/";
	
	private static final String[] CORPORA = new String[] {/*"cw09", "cw12",*/ "cc-2015-11"};
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus : CORPORA) {
				JavaRDD<String> input = context.textFile(DIR + corpus + "-calulated-edges-sampled-large-groups");
				
				samplePerBin(input)
					.saveAsTextFile(DIR + corpus + "-sampled-edges");
			}
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/sample-canonical-link-graph");
	
		return new JavaSparkContext(conf);
	}
	
	private static final int ELEMENTS_PER_BIN = 100;
	
	private static final List<String> BINS = Collections.unmodifiableList(bins());

	public static List<String> bins() {
		double delta = 0.05;
		List<String> ret = new ArrayList<>();
		double current = 0.4;
		
		while(current < 1d) {
			ret.add(String.format("%.4f", current));
			current += delta;
		}
		
		return ret;
	}

	public static String bin(String src) {
		double score = SparkAnalyzeCanonicalLinkGraph.extractS3Score(src) - 1e-10;
		
		for(int i= BINS.size()-1; i>= 0; i--) {
			String bin = BINS.get(i);
			
			if(Double.valueOf(bin) <= score) {
				return bin;
			}
		}
		
		return null;
	}

	public static JavaRDD<String> samplePerBin(JavaRDD<String> input) {
		JavaPairRDD<String, String> binToStr = input.mapToPair(i -> new Tuple2<>(bin(i), i));
		return binToStr.aggregateByKey(zeroValue(), (a,b) -> seqFunc(a,b), (a,b) -> combFunc(a,b))
				.map(i -> collect(i));
	}

	@SneakyThrows
	private static String collect(Tuple2<String, PriorityQueue<Tuple2<Double, String>>> i) {
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("s3Bucket", i._1());
		List<String> toAdd = new ArrayList<>(i._2().stream().map(j -> j._2()).collect(Collectors.toList()));
		Collections.sort(toAdd);
		List<Object> parsed = new ArrayList<>();
		
		for(String tmp: toAdd) {
			parsed.add(new ObjectMapper().readValue(tmp, Map.class));
		}
		
		ret.put("sample-edges", parsed);
		return new ObjectMapper().writeValueAsString(ret);
	}

	private static PriorityQueue<Tuple2<Double, String>> zeroValue() {
		return new PriorityQueue<Tuple2<Double,String>>(new TmpComp());
	}
	
	private static PriorityQueue<Tuple2<Double, String>> seqFunc(PriorityQueue<Tuple2<Double, String>> a, String b) {
		Tuple2<Double, String> tmp = new Tuple2<>(new Random().nextDouble(), b);
		a.add(tmp);
		
		if(a.size() > ELEMENTS_PER_BIN) {
			a.poll();
		}
		
		return a;
	}
	
	private static PriorityQueue<Tuple2<Double, String>> combFunc(PriorityQueue<Tuple2<Double, String>> a, PriorityQueue<Tuple2<Double, String>> b) {
		PriorityQueue<Tuple2<Double, String>> ret = zeroValue();
		ret.addAll(a);
		ret.addAll(b);
		
		while(ret.size() > ELEMENTS_PER_BIN) {
			ret.poll();
		}
		
		return ret;
	}
	
	@SuppressWarnings("serial")
	public static class TmpComp implements Comparator<Tuple2<Double, String>>, Serializable {
		@Override
		public int compare(Tuple2<Double, String> a, Tuple2<Double, String> b) {
			return a._1().compareTo(b._1());
		}
	}
}
