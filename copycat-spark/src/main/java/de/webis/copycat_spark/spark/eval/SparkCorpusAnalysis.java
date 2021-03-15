package de.webis.copycat_spark.spark.eval;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.copycat_spark.util.SourceDocuments.DocumentWithFingerprint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class SparkCorpusAnalysis {
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: new String[] {"combined"/*"cw09", "cw12", "cc-2015-11", "cc-2017-04"*/}) {
//				String input = "cikm2020/document-fingerprints-final/" + corpus +"-jsonl.bzip2";
				String input = "cikm2020/document-fingerprints-final/{cw,cc-2015}*-jsonl.bzip2";
				CorpusAnalysis ret = context.textFile(input)
						.map(src -> CorpusAnalysis.fromDocumentWithFingerprint(src, corpus))
						.reduce((a, b) -> reduce(a, b));
				
				long distinctCanonicalUrlCount = context.textFile(input)
						.map(src -> DocumentWithFingerprint.fromString(src).getCanonicalURL())
						.filter(i -> i != null)
						.map(i -> i.toString())
						.distinct()
						.count();
				
				ret.setDistinctCanonicalUrlCount(distinctCanonicalUrlCount);
				
				context.parallelize(Arrays.asList(ret))
					.saveAsTextFile("cikm2020/document-fingerprints-final/results/corpus-" + corpus + ".json");
			}
		}
	}
	
	public static CorpusAnalysis reduce(CorpusAnalysis a, CorpusAnalysis b) {
		return new CorpusAnalysis(a.getCorpus(),
				a.getDocumentCount() + b.getDocumentCount(),
				a.getUrlCount() + b.getUrlCount(),
				a.getCanonicalUrlCount() + b.getCanonicalUrlCount(),
				0l
		);
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class CorpusAnalysis implements Serializable {
		public String corpus;
		public long documentCount;
		public long urlCount;
		public long canonicalUrlCount;
		public long distinctCanonicalUrlCount;
		
		public static CorpusAnalysis fromDocumentWithFingerprint(String src, String corpus) {
			DocumentWithFingerprint doc = DocumentWithFingerprint.fromString(src);
			long urlCount = doc.getUrl() == null ? 0: 1;
			long canonicalUrlCount = doc.getCanonicalURL() == null ? 0: 1;
			
			return new CorpusAnalysis(corpus, 1, urlCount, canonicalUrlCount, 0);
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
