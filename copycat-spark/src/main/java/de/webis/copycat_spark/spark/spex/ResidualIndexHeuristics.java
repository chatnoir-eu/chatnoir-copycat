package de.webis.copycat_spark.spark.spex;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;

import com.google.common.collect.ImmutableList;

import de.webis.copycat_spark.spark.spex.ResidualIndex.ResidualIndexEntry;
import de.webis.trec_ndd.spark.DocumentHash;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Tuple2;

public class ResidualIndexHeuristics {
	
	public static List<ResidualIndexHeuristic> sortedHeuristics(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex, double threshold) {
		JavaRDD<ResidualIndexHeuristic> ret = heuristics(metadata, residualIndex);
		ret = ret.filter(i -> i.getUpperS3Bound() >= threshold);
		ret = ret.sortBy(i -> i.docLength, true, 0);
		
		return ret.collect();
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
}
