package de.webis.copycat_spark.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.ChunkSelectionStrategy;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 * This calculates S3 similarities for all documents in a corpus with SPARK
 */
public class FullS3Deduplication {
	
	public static void main(String[] args) {
		Namespace parsedArgs = parseArgs(args);
		if(parsedArgs == null) {
			return;
		}
		
		List<CollectionDocument> docs = new ArrayList<>();
		for(Object jsonl: parsedArgs.getList(ArgumentParsingUtil.ARG_INPUT)) {
			System.out.println("Process " + jsonl);
			List<CollectionDocument> tmp = docs((String) jsonl);
			System.out.println("Read " + tmp.size() + " documents from " + (String) jsonl);
			
			docs.addAll(tmp);
		}
		
		System.out.println("Start deduplication with " + docs.size() + " documents.");
		
		try(JavaSparkContext jsc = context()) {
			JavaRDD<CollectionDocument> docsRdd = jsc.parallelize(docs);
			String index = parsedArgs.getString("eightGramIndex");
			String s3Scores = parsedArgs.getString("s3Scores");
			
			build8GrammIndex(docsRdd, jsc, index);
			calculateS3Scores(jsc, index, s3Scores, docsRdd);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": Repartition");

		return new JavaSparkContext(conf);
	}
	
	static Namespace parseArgs(String[] args) {
		ArgumentParser parser = argParser();
		
		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Run a Full S3 Deduplication of a dataset.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.nargs("+")
			.help("Jsonl files that contain documents.")
			.required(true);
		
		ret.addArgument("--eightGramIndex")
			.help("The intermediate 8-gramm index is stored here.")
			.required(true);
		
		ret.addArgument("--s3Scores")
			.help("The resulting s3Scores are stored here.")
			.required(true);
		
		return ret;
	}
	
	private static void calculateS3Scores(JavaSparkContext jsc, String eightGrammIndexDir, String outputDir, JavaRDD<CollectionDocument> docs) {
		if(fileExists(outputDir +"/_SUCCESS", jsc)) {
			return;
		}
		
		JavaRDD<Word8GrammIndexEntry> allIndexEntries = jsc.textFile(eightGrammIndexDir)
					.map(Word8GrammIndexEntry::fromString);
		
		JavaRDD<S3ScoreIntermediateResult> intermediateS3 = sumCoocurrencesOfAllIndexEntries(allIndexEntries);
		
		JavaPairRDD<String, DocumentHash> metadata = documentMetadata(docs);
		
		intermediateS3 = joinMetadataOfLeftDocument(intermediateS3, metadata);
		intermediateS3 = joinMetadataOfRightDocument(intermediateS3, metadata);
		
		intermediateS3.map(i -> new S3Score(i))
			.saveAsTextFile(outputDir);
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
	
	private static JavaPairRDD<String, DocumentHash> documentMetadata(JavaRDD<CollectionDocument> docs) {
		return docs
				.map(i -> new DocumentHash(i))
				.mapToPair(d -> new Tuple2<>(d.getId(), d));
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> sumCoocurrencesOfAllIndexEntries(JavaRDD<Word8GrammIndexEntry> allIndexEntries) {
		return allIndexEntries
				.flatMap(indexEntry -> SymmetricPairUtil.extractCoocurrencePairs(indexEntry).iterator())
				.groupBy(Pair::getLeft)
				.map(FullS3Deduplication::sum);
	}
	
	private static S3ScoreIntermediateResult sum(Tuple2<Pair<String, String>, Iterable<Pair<Pair<String, String>, Integer>>> v) {
		int sum = 0;
		Iterator<Pair<Pair<String, String>, Integer>> iter = v._2().iterator();
		
		while(iter.hasNext()) {
			sum += iter.next().getRight();
		}
		
		return new S3ScoreIntermediateResult()
				.setCommonNGramms(sum)
				.setIdPair(v._1());
	}
	
	private static void build8GrammIndex(JavaRDD<CollectionDocument> docs, JavaSparkContext context, String outputDir) {
		if(fileExists(outputDir +"/_SUCCESS", context)) {
			return;
		}

		ChunkSelectionStrategy chunkSelection = ChunkSelectionStrategy.SPEX;
		
		docs.flatMap(FullS3Deduplication::documentTo8Gramms)
			.groupBy(Tuple2::_1)
			.map(Word8GrammIndexEntry::buildIndexEntry)
			.filter(c -> chunkSelection.getKeepIndexEntry().apply(c))
			.saveAsTextFile(outputDir);
	}
	
	private static Iterator<Tuple2<Word8Gramm, String>> documentTo8Gramms(CollectionDocument doc) {
		List<Tuple2<Word8Gramm, String>> ret = new LinkedList<>();
		String id = doc.getId();
		
		for(Word8Gramm nGramm :  NGramms.build8Gramms(doc.getFullyCanonicalizedContent())) {
			ret.add(new Tuple2<>(nGramm, id));
		}
		
		return ret.iterator();
	}
	
	private static boolean fileExists(String file, JavaSparkContext jsc) {
		try {
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
		
			return fs.exists(new org.apache.hadoop.fs.Path(file));
		} catch (IOException e) {
			return false;
		}
	}

	@SneakyThrows
	public static List<CollectionDocument> docs(String jsonlFile) {
		List<CollectionDocument> ret = new ArrayList<>();
		
		for(String line: Files.readAllLines(Paths.get(jsonlFile))) {
			ret.add(CollectionDocument.fromString(line));
		}
		
		return ret;
	}
}
