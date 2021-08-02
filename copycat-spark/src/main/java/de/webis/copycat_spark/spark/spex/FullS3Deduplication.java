package de.webis.copycat_spark.spark.spex;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.copycat_spark.spark.spex.ResidualIndex.ResidualIndexEntry;
import de.webis.copycat_spark.spark.spex.ResidualIndexHeuristics.ResidualIndexHeuristic;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.ChunkSelectionStrategy;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import lombok.Data;
import lombok.SneakyThrows;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 * This calculates S3 similarities for all documents in a corpus with SPARK
 */
@Data
@SuppressWarnings("serial")
public class FullS3Deduplication implements Serializable {
	
	private final SpexConfiguration config;
	
	public static void main(String[] args) {
		SpexConfiguration config = SpexConfiguration.parseSpexConfiguration(args);
		if(config == null) {
			return;
		}
		
		try(JavaSparkContext jsc = context()) {
			new FullS3Deduplication(config).runSpexDeduplication(jsc);
		}
	}
	
	public void runSpexDeduplication(JavaSparkContext jsc) {
		JavaRDD<CollectionDocument> documents = collectionDocuments(jsc);
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(documents, jsc);
		
		buildIndex(documents, jsc);
		buildResidualIndex(jsc);
		calculateIntermediateScores(documentMetadata, jsc);
		
		JavaPairRDD<String, ResidualIndexEntry> docToResidualIndexEntry = residualIndexEntry(jsc);
		finalizeScores(docToResidualIndexEntry, jsc);
		documentPairsInResidualIndexAboveThreshold(docToResidualIndexEntry, documentMetadata, jsc);
	}
	
	private void documentPairsInResidualIndexAboveThreshold(JavaPairRDD<String, ResidualIndexEntry> docToResidualIndexEntry, JavaPairRDD<String, DocumentHash> documentMetadata, JavaSparkContext jsc) {
		if(fileExists(config.getPairsToRecalculateInResidualIndexDirectory() +"/_SUCCESS", jsc)) {
			return;
		}
		
		List<ResidualIndexHeuristic> heuristics = ResidualIndexHeuristics.sortedHeuristics(documentMetadata, docToResidualIndexEntry, config.getThreshold());
		JavaRDD<String> ret = ResidualIndexHeuristics.extractCandidates(jsc, heuristics, config.getThreshold());
		ret.saveAsTextFile(config.getPairsToRecalculateInResidualIndexDirectory());
	}

	private JavaPairRDD<String, ResidualIndexEntry> residualIndexEntry(JavaSparkContext jsc) {
		return residualIndexEntry(jsc.textFile(config.getResidualIndexDirectory()));
	}

	public static JavaPairRDD<String, ResidualIndexEntry> residualIndexEntry(JavaRDD<String> indexEntries) {
		return indexEntries
			.map(i -> ResidualIndexEntry.fromString(i))
			.mapToPair(i -> new Tuple2<>(i.getDocumentId(), i));
	}
	
	private void buildResidualIndex(JavaSparkContext jsc) {
		if(fileExists(config.getResidualIndexDirectory() +"/_SUCCESS", jsc)) {
			return;
		}
		
		JavaRDD<Word8GrammIndexEntry> tooLargeIndexEntries = jsc.textFile(config.getIndexDirectory())
			.map(Word8GrammIndexEntry::fromString)
			.filter(i -> isValid(i) && isTooLarge(i));
		JavaRDD<ResidualIndexEntry> residualIndex = ResidualIndex.residualIndexEntries(tooLargeIndexEntries);
		
		residualIndex.map(i -> i.toString())
			.repartition(config.getMetadataPartitionCount())
			.saveAsTextFile(config.getResidualIndexDirectory());
	}
	
	private JavaRDD<CollectionDocument> collectionDocuments(JavaSparkContext jsc) {
		return jsc.textFile(config.getInput())
			.map(i -> parse(i))
			.filter(i -> i != null);
	}
	
	private static CollectionDocument parse(String json) {
		try {
			return CollectionDocument.fromString(json);
		} catch (Exception e) {
			return null;
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": SPEX");

		return new JavaSparkContext(conf);
	}
	
	private void finalizeScores(JavaPairRDD<String, ResidualIndexEntry> docToResidualIndexEntry, JavaSparkContext jsc) {
		if(fileExists(config.getFinalScoreDirectory() +"/_SUCCESS", jsc)) {
			return;
		}
		
		Map<String, Integer> docToTokensInResidualIndex = new LinkedHashMap<>(docToResidualIndexEntry
			.mapToPair(i -> new Tuple2<>(i._1(), i._2().getResidualTokens().size()))
			.collectAsMap());
		
		jsc.textFile(config.getIntermediateScoreDirectory())
			.map(i -> S3Score.fromString(i))
			.map(i -> finalizeScore(i, docToTokensInResidualIndex))
			.saveAsTextFile(config.getFinalScoreDirectory());
	}
	
	@SneakyThrows
	private static String finalizeScore(S3Score s3, Map<String, Integer> docToTokensInResidualIndex) {
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("s3", s3);
		
		ret.put("chunksInAInResidualIndex", docToTokensInResidualIndex.getOrDefault(s3.getIdPair().getLeft(), 0));
		ret.put("chunksInBInResidualIndex", docToTokensInResidualIndex.getOrDefault(s3.getIdPair().getRight(), 0));
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	private void calculateIntermediateScores(JavaPairRDD<String, DocumentHash> metadata, JavaSparkContext jsc) {
		if(fileExists(config.getIntermediateScoreDirectory() +"/_SUCCESS", jsc)) {
			return;
		}
		
		JavaRDD<Word8GrammIndexEntry> allIndexEntries = jsc.textFile(config.getIndexDirectory())
					.map(Word8GrammIndexEntry::fromString);
		
		JavaRDD<S3ScoreIntermediateResult> intermediateS3 = sumCoocurrencesOfAllIndexEntries(allIndexEntries);
		
		intermediateS3 = joinMetadataOfLeftDocument(intermediateS3, metadata);
		intermediateS3 = joinMetadataOfRightDocument(intermediateS3, metadata);
		
		intermediateS3.map(i -> new S3Score(i))
			.saveAsTextFile(config.getIntermediateScoreDirectory());
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> joinMetadataOfLeftDocument(JavaRDD<S3ScoreIntermediateResult> intermediateS3, JavaPairRDD<String, DocumentHash> metadata) {
		JavaPairRDD<String, Tuple2<S3ScoreIntermediateResult, DocumentHash>> joined = intermediateS3
				.mapToPair(i -> new Tuple2<String, S3ScoreIntermediateResult>(i.getIdPair().getLeft(), i))
				.join(metadata);
		
		return joined.map(i -> {
			S3ScoreIntermediateResult ret = i._2._1;
			ret.setLeftMetadata(i._2._2);

			return ret;
		});
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> joinMetadataOfRightDocument(JavaRDD<S3ScoreIntermediateResult> intermediateS3, JavaPairRDD<String, DocumentHash> metadata) {
		JavaPairRDD<String, Tuple2<S3ScoreIntermediateResult, DocumentHash>> joined = intermediateS3
				.mapToPair(i -> new Tuple2<String, S3ScoreIntermediateResult>(i.getIdPair().getRight(), i))
				.join(metadata);
		
		return joined.map(i -> {
			S3ScoreIntermediateResult ret = i._2._1;
			ret.setRightMetadata(i._2._2);

			return ret;
		});
	}
	
	private JavaPairRDD<String, DocumentHash> documentMetadata(JavaRDD<CollectionDocument> docs, JavaSparkContext jsc) {
		if(!fileExists(config.getDocumentMetadataDirectory() +"/_SUCCESS", jsc)) {
			docs.map(i -> new DocumentHash(i).toString())
				.repartition(config.getMetadataPartitionCount())
				.saveAsTextFile(config.getDocumentMetadataDirectory());
		}
		
		return documentMetadata(jsc.textFile(config.getDocumentMetadataDirectory()));	
	}
	
	public static JavaPairRDD<String, DocumentHash> documentMetadata(JavaRDD<String> documentHashes) {
		return documentHashes.map(i -> DocumentHash.fromString(i))
				.filter(i -> i != null)
				.mapToPair(d -> new Tuple2<>(d.getId(), d));
	}
	
	private JavaRDD<S3ScoreIntermediateResult> sumCoocurrencesOfAllIndexEntries(JavaRDD<Word8GrammIndexEntry> allIndexEntries) {
		JavaPairRDD<Pair<String, String>, Integer> tmp = allIndexEntries
			.flatMapToPair(indexEntry -> extractCoocurrencePairs(indexEntry).iterator());
		
		tmp = tmp.reduceByKey((a,b ) -> a+b);
		
		return tmp.map(i -> new S3ScoreIntermediateResult()
				.setCommonNGramms(i._2())
				.setIdPair(i._1()));
	}
	
	private List<Tuple2<Pair<String, String>, Integer>> extractCoocurrencePairs(Word8GrammIndexEntry input) {
		if(!isValid(input) || isTooLarge(input)) {
			return Collections.emptyList();
		}
		
		return SymmetricPairUtil.extractCoocurrencePairs(input).stream()
			.map(i -> new Tuple2<>(i.getLeft(), i.getRight()))
			.collect(Collectors.toList());
	}
	
	private boolean isValid(Word8GrammIndexEntry indexEntry) {
		return indexEntry != null && indexEntry.getDocumentIds() != null;
	}
	
	private boolean isTooLarge(Word8GrammIndexEntry indexEntry) {
		return indexEntry.getDocumentIds().size() > config.getPostlistThresholdForAllPairsCalculation();
	}
	
	private void buildIndex(JavaRDD<CollectionDocument> docs, JavaSparkContext jsc) {
		if(fileExists(config.getIndexDirectory() +"/_SUCCESS", jsc)) {
			return;
		}

		ChunkSelectionStrategy chunkSelection = ChunkSelectionStrategy.SPEX;
		
		docs.flatMap(FullS3Deduplication::documentTo8Gramms)
			.groupBy(Tuple2::_1)
			.map(Word8GrammIndexEntry::buildIndexEntry)
			.filter(c -> chunkSelection.getKeepIndexEntry().apply(c))
			.saveAsTextFile(config.getIndexDirectory());
	}
	
	private static Iterator<Tuple2<Word8Gramm, String>> documentTo8Gramms(CollectionDocument doc) {
		List<Tuple2<Word8Gramm, String>> ret = new LinkedList<>();
		String id = doc.getId();
		
		for(Word8Gramm nGramm :  NGramms.build8Gramms(doc.getFullyCanonicalizedContent())) {
			ret.add(new Tuple2<>(nGramm, id));
		}
		
		return ret.iterator();
	}
	
	private boolean fileExists(String file, JavaSparkContext jsc) {
		try {
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
		
			return fs.exists(new org.apache.hadoop.fs.Path(file));
		} catch (IOException e) {
			return false;
		}
	}
}
