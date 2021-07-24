package de.webis.copycat_spark.spark.spex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableList;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class ResidualIndex {

	public static JavaRDD<ResidualIndexEntry> residualIndexEntries(JavaRDD<Word8GrammIndexEntry> index) {
		JavaPairRDD<String, String> docToResidualToken = index.flatMapToPair(i -> docToResidualTokens(i));

		return docToResidualToken.groupByKey().map(i -> residualIndexEntry(i));
	}
	
	private static ResidualIndexEntry residualIndexEntry(Tuple2<String, Iterable<String>> i) {
		return new ResidualIndexEntry(i._1(), new ArrayList<>(ImmutableList.copyOf(i._2())));
	}

	private static Iterator<Tuple2<String, String>> docToResidualTokens(Word8GrammIndexEntry i) {
		List<Tuple2<String, String>> ret = new LinkedList<>();
		if(i == null || i.getWord8Gramm() == null || i.getWord8Gramm().getNGramm() == null || i.getDocumentIds() == null) {
			return Collections.emptyIterator();
		}
		
		for(String docId: i.getDocumentIds()) {
			ret.add(new Tuple2<>(docId, i.getWord8Gramm().getNGramm()));
		}
		
		return ret.iterator();
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class ResidualIndexEntry implements Serializable {
		private String documentId;
		private List<String> residualTokens;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static ResidualIndexEntry fromString(String str) {
			return new ObjectMapper().readValue(str, ResidualIndexEntry.class);
		}
	}
}
