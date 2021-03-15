package de.webis.copycat_spark.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterators;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat_spark.app.SampleNearDuplicates.NearDuplicate;
import de.webis.copycat_spark.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.copycat_spark.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.copycat_spark.util.CollectionDocumentUtil;
import de.webis.copycat_spark.util.TakeRandom;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class EnrichSimHashNearDuplicatesWithS3Similarity {
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> inputCsv = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			enrichNearDuplicatesWithS3Score(inputCsv, docResolver(parsedArgs), parsedArgs.getString(ArgumentParsingUtil.ARG_FORMAT))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	public static JavaRDD<String> enrichNearDuplicatesWithS3Score(JavaRDD<String> simHashNearDuplicates, DocumentResolverFactory docResolverFactory, String format) {
		Format f = Format.getFormat(format);
		return simHashNearDuplicates
				.mapToPair(i -> new Tuple2<>(f.firstId(i), i))
				.groupByKey()
				.flatMap(i -> enrichS3ScoreFailSave(i, docResolverFactory, f));
	}
	
	private static Iterator<String> enrichS3ScoreFailSave(Tuple2<String, Iterable<String>> groupForFirstId, DocumentResolverFactory docResolverFactory, Format f) {
		try {
			return enrichS3Score(groupForFirstId, docResolverFactory, f);
		} catch (Error e) {
			return Iterators.transform(groupForFirstId._2().iterator(), i -> "{\"firstId\":\"" + f.firstId(i) + "\",\"secondId\":\"" + f.secondId(i) + "\",\"s3Score\":-1}"); 
		}
	}
	
	static Iterator<String> enrichS3Score(Tuple2<String, Iterable<String>> groupForFirstId, DocumentResolverFactory docResolverFactory, Format f) {
		DocumentResolver docResolver = docResolverFactory.get();
		String firstId = groupForFirstId._1();
		CollectionDocument firstDoc = docResolver.loadCollectionDocument(firstId);
		DocumentHash firstHash = firstDoc == null ? null : new DocumentHash(firstDoc);
		Set<Word8Gramm> firstWord8Gramms = firstDoc == null ? null : SparkEnrichRelevanceTransferPairs.word8Gramms(firstDoc);
		
		List<String> groups = TakeRandom.takeRandomElements(100000, groupForFirstId._2());
		
		if(EnrichPairsOfDocumentsWithS3SCore.CALCULATE_ONLY_S3) {
			if(firstId.startsWith("clueweb09")) {
				groups = groups.stream().filter(i -> i.startsWith("clueweb09")).collect(Collectors.toList());
			} else if(firstId.startsWith("clueweb12")) {
				groups = groups.stream().filter(i -> i.startsWith("clueweb12")).collect(Collectors.toList());
			} else {
				groups = new ArrayList<>();
			}
		}
		
		if(EnrichPairsOfDocumentsWithS3SCore.CALCULATE_ONLY_S3) {
			return Iterators.transform(
					groups.iterator(),
					i -> addS3ScoreToCsvLine(firstHash, firstWord8Gramms, i, docResolver, f)
				);
		} else {
			return Iterators.transform(
				groups.iterator(),
				i -> addS3ScoreToCsvLine(firstDoc, i, docResolver, f)
			);
		}
	}
	
	private static String addS3ScoreToCsvLine(DocumentHash firstHash, Set<Word8Gramm> firstWord8Gramms, String csvLine, DocumentResolver docResolver, Format f) {
		String secondId = f.secondId(csvLine);
		CollectionDocument secondDoc = docResolver.loadCollectionDocument(secondId); 
		String  s3Score = "-1";
		
		
		if(firstHash != null && secondDoc != null) {
			s3Score = String.format("%.4f", SparkEnrichRelevanceTransferPairs.s3Score(firstHash, firstWord8Gramms, secondDoc));
		}
		
		return "{\"firstId\":\"" + f.firstId(csvLine) + "\",\"secondId\":\"" + secondId + "\",\"s3Score\":" +
			s3Score + "}";
	}
	
	static String addS3ScoreToCsvLine(CollectionDocument firstDoc, String csvLine, DocumentResolver docResolver, Format f) {
		String secondId = f.secondId(csvLine);
		String firstId = f.firstId(csvLine);
		CollectionDocument secondDoc = docResolver.loadCollectionDocument(secondId); 
		String  s3Score = "-1",
				cosineSimilarityOneGramms = "-1",
				cosineSimilarityEightGramms = "-1",
				cosineSimilarityThreeAndFiveGramms = "-1";
		
		
		if(firstDoc != null && secondDoc != null) {
			s3Score = String.format("%.4f", SparkEnrichRelevanceTransferPairs.s3Score(firstDoc, secondDoc));
			
			HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(firstDoc), 64);
			HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(secondDoc), 64);

			cosineSimilarityThreeAndFiveGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
			
			aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDoc, 8), 64);
			bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDoc, 8), 64);
			
			cosineSimilarityEightGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
			
			aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDoc, 1), 64);
			bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDoc, 1), 64);

			cosineSimilarityOneGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
		}
		
		return "{\"firstId\":\"" + firstId + "\",\"secondId\":\"" + secondId + "\",\"s3Score\":" +
			s3Score + ",\"cosineSimilarityOneGramms\":" + cosineSimilarityOneGramms + ",\"cosineSimilarityEightGramms\":"
			+ cosineSimilarityEightGramms +",\"cosineSimilarityThreeAndFiveGramms\":" + cosineSimilarityThreeAndFiveGramms + "}";
	}
	
	private static void sleepSometimesToPreventNetworkProblems(int i) {
		//sounds silly, but is true: 
		if(i % 100 == 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
	}
	
	static DocumentResolverFactory docResolver(Namespace args) {
		return () -> CollectionDocumentUtil.HdfsMapFileDocumentResolver.smartDocumentResolver();
	}
	
	public static interface DocumentResolverFactory extends Supplier<DocumentResolver>, Serializable {};
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": SampleNearDuplicates");

		return new JavaSparkContext(conf);
	}
	
	static Namespace validArgumentsOrNull(String[] args) {
		ArgumentParser parser = argParser();

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": EnrichSimHashNearDuplicatesWithS3Similarity")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"Near-duplicate file in csv format");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting csv enriched with an additional s3Score column is stored under this location.");
		
		ret.addArgument("-f", "--" + ArgumentParsingUtil.ARG_FORMAT)
			.required(Boolean.FALSE)
			.setDefault("json")
			.choices("json", "csv");
		return ret;
	}
	
	static interface Format extends Serializable {
		public String firstId(String input);
		public String secondId(String input);
		
		@SuppressWarnings("serial")
		static Format JSON_FORMAT = new Format() {
			@Override
			public String firstId(String input) {
				NearDuplicate nd = NearDuplicate.fromString(input);

				return nd.getFirstId();
			}

			@Override
			public String secondId(String input) {
				NearDuplicate nd = NearDuplicate.fromString(input);

				return nd.getSecondId();
			}
		};
		
		@SuppressWarnings("serial")
		static Format CSV_FORMAT = new Format() {
			@Override
			public String firstId(String input) {
				return StringUtils.substringBefore(input, ",");
			}

			@Override
			public String secondId(String input) {
				return StringUtils.substringAfter(input, ",");
			}
		};
		
		public static Format getFormat(String format) {
			if("json".equalsIgnoreCase(format)) {
				return JSON_FORMAT;
			} else if ("csv".equalsIgnoreCase(format)) {
				return CSV_FORMAT;
			}
			
			throw new RuntimeException("Could not handle format: " + format);
		}
	}
}
