package de.webis.cikm20_duplicates.spark.eval;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class SparkCorpusAnalysis {
	
	private static final String CORPUS = "cw12";
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			CorpusAnalysis ret = context.textFile("cikm2020/document-fingerprints-final/" + CORPUS +"-jsonl.bzip2")
					.map(src -> CorpusAnalysis.fromDocumentWithFingerprint(src))
					.reduce((a, b) -> reduce(a, b));
			
			context.parallelize(Arrays.asList(ret))
				.saveAsTextFile("cikm2020/document-fingerprints-final/results/corpus-" + CORPUS + ".json");
		}
	}
	
	public static CorpusAnalysis reduce(CorpusAnalysis a, CorpusAnalysis b) {
		return new CorpusAnalysis(a.getCorpus(),
				a.getDocumentCount() + b.getDocumentCount(),
				a.getUrlCount() + b.getUrlCount(),
				a.getCanonicalUrlCount() + b.getCanonicalUrlCount()
		);
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class CorpusAnalysis implements Serializable {
		public String corpus;
		public int documentCount;
		public int urlCount;
		public int canonicalUrlCount;
		
		public static CorpusAnalysis fromDocumentWithFingerprint(String src) {
			DocumentWithFingerprint doc = DocumentWithFingerprint.fromString(src);
			int urlCount = doc.getUrl() == null ? 0: 1;
			int canonicalUrlCount = doc.getCanonicalURL() == null ? 0: 1;
			
			return new CorpusAnalysis(CORPUS, 1, urlCount, canonicalUrlCount);
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/corpus-analysis");
	
		return new JavaSparkContext(conf);
	}
}
