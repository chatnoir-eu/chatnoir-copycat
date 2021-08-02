package de.webis.copycat_spark.spark.spex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableList;

import de.webis.copycat_spark.spark.spex.ResidualIndex.ResidualIndexEntry;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class ResidualIndexHeuristics {
	
	public static List<ResidualIndexHeuristic> sortedHeuristics(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex, double threshold) {
		JavaRDD<ResidualIndexHeuristic> ret = heuristics(metadata, residualIndex);
		ret = ret.filter(i -> i.getUpperS3Bound() >= threshold);
		ret = ret.sortBy(i -> i.docLength, true, 0);
		
		return ret.collect();
	}
	
	public static boolean canBeAboveThreshold(ResidualIndexHeuristic a, ResidualIndexHeuristic b, double threshold) {
		S3ScoreIntermediateResult ret = new S3ScoreIntermediateResult();
		ret.setCommonNGramms(Math.min(a.residualTokens, b.residualTokens));
		
		ret.setLeftMetadata(pseudoHash(a));
		ret.setRightMetadata(pseudoHash(b));
		
		return new S3Score(ret).getS3Score() >= threshold;
	}
	
	private static DocumentHash pseudoHash(ResidualIndexHeuristic a) {
		DocumentHash ret = new DocumentHash();
		ret.setFullyCanonicalizedWord8GrammSetSize(a.getDocLength());
		
		return ret;
	}
	
	public static JavaRDD<ResidualIndexHeuristic> heuristics(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex) {
		JavaPairRDD<String, Integer> docToResidualIndexEntries = residualIndex.mapToPair(i -> new Tuple2<>(i._1(), length(i._2())));
		
		return docToResidualIndexEntries.leftOuterJoin(metadata).groupByKey().map(i -> new ResidualIndexHeuristic(i));
	}
	
	private static int length(ResidualIndexEntry entry) {
		Set<String> ret = new HashSet<>(entry.getResidualTokens());
		
		return ret.size();
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class ResidualIndexHeuristic implements Serializable {
		String documentId;
		int residualTokens;
		int docLength;
		
		public Float getUpperS3Bound() {
			return (2*((float)residualTokens))/((float) docLength);
		}
		
		public int getOtherDocRange(double threshold) {
			int maxOtherLength = (int) Math.ceil(((((double)residualTokens)*2)/threshold)- ((double) docLength) + 1d);
			
			return Math.max(0, maxOtherLength - docLength);
		}

		public ResidualIndexHeuristic(Tuple2<String, Iterable<Tuple2<Integer, Optional<DocumentHash>>>> i) {
			this.documentId = i._1();
			List<Tuple2<Integer, Optional<DocumentHash>>> data = ImmutableList.copyOf(i._2().iterator());
			if(data.size() != 1 || !data.get(0)._2.isPresent()) {
				throw new RuntimeException("I cant handle " + data + " for id '" + documentId + "'.");
			}
			
			this.residualTokens = data.get(0)._1();
			this.docLength = (int) data.get(0)._2.get().getFullyCanonicalizedWord8GrammSetSize();
		}
	}

	public static JavaRDD<String> extractCandidates(JavaSparkContext jsc, List<ResidualIndexHeuristic> input, double threshold) {
		List<Integer> tasks = new ArrayList<>();
		
		for(int i=0; i< input.size(); i++) {
			failIfEntryIsInvalid(input, i);
			tasks.add(i);
		}
		
		return jsc.parallelize(tasks)
				.map(i -> runLoopForTask(i, input, threshold));
	}
	
	@SneakyThrows
	private static String runLoopForTask(Integer position, List<ResidualIndexHeuristic> input, double threshold) {
		List<Pair<String, String>> pairsToCalculate = new ArrayList<>();
		int consideredPairs = 0;
		ResidualIndexHeuristic doc = input.get(position);
		int stopAtLength = doc.docLength + doc.getOtherDocRange(threshold);
		
		for(int i= position+1; i< input.size(); i++) {
			consideredPairs++;
			ResidualIndexHeuristic curr = input.get(i);
			
			if(stopAtLength < curr.docLength) {
				break;
			}
			
			if(canBeAboveThreshold(doc, curr, threshold)) {
				pairsToCalculate.add(SymmetricPairUtil.of(doc.getDocumentId(), curr.getDocumentId()));
			}
		}
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("position", position);
		ret.put("threshold", threshold);
		ret.put("consideredPairs", consideredPairs);
		ret.put("pairsToCalculate", pairsToCalculate);
		
		return new ObjectMapper().writeValueAsString(ret);
	}

	private static void failIfEntryIsInvalid(List<ResidualIndexHeuristic> input, int pos) {
		if (pos > 0) {
			ResidualIndexHeuristic current = input.get(pos);
			ResidualIndexHeuristic previous = input.get(pos-1);
			
			if (current.docLength < previous.docLength) {
				throw new RuntimeException("Can not handle: " + current + " and " + previous);
			}
		}
	}
}
