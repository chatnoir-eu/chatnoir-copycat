package de.webis.cikm20_duplicates.app.url_groups;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import de.webis.cikm20_duplicates.app.ArgumentParsingUtil;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationStrategy;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import de.webis.cikm20_duplicates.util.ClientLocalDeduplication;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.similarity.MD5;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

/**
 * This spark job assumes that documents with the same url and/or canonical url are in the same partition
 * (i.e. each document may occur twice in the partition:
 *       (1) emitted with its url as key, and
 *       (2) emitted with its canonical-url as key
 * ). repartitions all documents so that documents with the same URL or same canonical URL belong to the same partition.
 * 
 * 
 * @author Maik Fröbe
 *
 */
public class CreateUrlDeduplicationCandidates {
	
	private static final String PSEUDO_FINGERPRINT_NAME = "pseudo-fingerprint";
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			exactDuplicates(input, parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS))
				.saveAsTextFile(parsedArgs.get(ArgumentParsingUtil.ARG_OUTPUT) + "exact-duplicates", BZip2Codec.class);
			
			createDeduplicationtasks(input, parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS))
				.saveAsTextFile(parsedArgs.get(ArgumentParsingUtil.ARG_OUTPUT) + "near-duplicate-tasks", BZip2Codec.class);
		}
	}
	
	static JavaRDD<String> exactDuplicates(JavaRDD<String> input, int partitions) {
		input = input.map(i -> DocumentForUrlDeduplication.fromString(i))
				.map(i -> toDocumentRepresentationForExactHashDuplicatesWithSameUrlOrCanonicalUrl(i));
		
		return SparkCreateDeduplicationCandidates.exactDuplicates(input, deduplicationStrategyForExactDuplicates(partitions));
	}
	
	public static JavaRDD<String> createDeduplicationtasks(JavaRDD<String> docsWithFingerprint, int partitions) {
		DeduplicationStrategy f = deduplicationStrategyForExactDuplicates(partitions);
		JavaPairRDD<Pair<String, Integer>, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.groupBy(i -> i._1())
			.flatMap(i -> workingPackagesPerUrl(i._2()))
			.repartition(f.numPartitions());
	}
	
	private static Iterator<String> workingPackagesPerUrl(Iterable<Tuple2<Pair<String, Integer>, DeduplicationUnit>> group) {
		Iterator<Tuple2<Integer, DeduplicationUnit>> mappedGroupIterator = Iterators.transform(group.iterator(), i -> new Tuple2<>(i._1().getRight(), i._2()));
		List<Tuple2<Integer, DeduplicationUnit>> mappedGroup = ImmutableList.copyOf(mappedGroupIterator);
		
		return ClientLocalDeduplication.workingPackages(mappedGroup);
	}
	
	private static JavaPairRDD<Pair<String, Integer>, DeduplicationUnit> hashPartitionToDocument(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		return docsWithFingerprint
				.map(i -> DocumentForUrlDeduplication.fromString(i))
				.flatMapToPair(doc -> extractHashesToDocId(doc))
				.repartitionAndSortWithinPartitions(new HashPartitioner(f.numPartitions()));
	}
	
	private static Iterator<Tuple2<Pair<String, Integer>, DeduplicationUnit>> extractHashesToDocId(DocumentForUrlDeduplication doc) {		
		String url = doc.getCanonicalURL().toString();
		
		return doc.getSimHash64BitK3OneGramms().stream()
				.map(hash -> new Tuple2<>(Pair.of(url, hash), new DeduplicationUnit(doc.getDocId(), doc.getSimHash64BitK3OneGramms())))
				.iterator();
	}
	
	@SuppressWarnings("serial")
	private static DeduplicationStrategy deduplicationStrategyForExactDuplicates(int partitions) {
		return new DeduplicationStrategy() {
			@Override
			public int numPartitions() {
				return partitions;
			}
			
			@Override
			public String name() {
				return PSEUDO_FINGERPRINT_NAME;
			}
		};
	}

	/**
	 * This mapping ensures that I can reuse {@link de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates#exactDuplicates} (additionally, it is tested in multiple integration tests that this mapping works as expected).
	 * The idea is that only exact hash-duplicates with the same url and/or canonical-url are recognized as exact duplicates here.
	 * To achieve that, I put the url/canonical url as part of the hash (simply add hashes of the url/canonical-url to the fingerprint) to ensure that only documents with the same url/canonical url and exact the same hash are recognized as duplicates.
	 * 
	 * @param doc
	 * @return
	 */
	static String toDocumentRepresentationForExactHashDuplicatesWithSameUrlOrCanonicalUrl(DocumentForUrlDeduplication doc) {
		DocumentWithFingerprint ret = new DocumentWithFingerprint();
		ret.setCanonicalURL(doc.getCanonicalURL());
		ret.setDocId(doc.getDocId());
		ret.setFingerprints(fingerprintsForExactDuplicatesWithSameUrlOrCanonicalUrl(doc));
		
		return ret.toString();
	}
	
	private static LinkedHashMap<String, ArrayList<Integer>> fingerprintsForExactDuplicatesWithSameUrlOrCanonicalUrl(DocumentForUrlDeduplication doc) {
		ArrayList<Integer> hash = hashesForUrl(doc.getCanonicalURL());
		hash.addAll(doc.getSimHash64BitK3OneGramms());
		
		LinkedHashMap<String, ArrayList<Integer>> ret = new LinkedHashMap<>();
		ret.put(PSEUDO_FINGERPRINT_NAME, hash);
		
		return ret;
	}
	
	/**
	 * 
	 * @return multiple hashes of the passed url (to reduce the probability of collissions.
	 */
	private static ArrayList<Integer> hashesForUrl(URL url) {
		String ret = url.toString();

		return new ArrayList<>(Arrays.asList(
			ret.hashCode(),
			("foo-bar" + ret + "bar-foo").hashCode(),
			("bar-foo" + ret + "foo-bar").hashCode(),
			MD5.md5hash(ret).hashCode(),
			MD5.md5hash("bar-foo" + ret + "foo-bar").hashCode()
		));
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateUrlDeduplicationCandidates");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateDeduplicationCandidates")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that contains all document representations.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting deduplication tasks are stored under this location.");

		ret.addArgument("--" + ArgumentParsingUtil.ARG_PARTITIONS).required(Boolean.TRUE)
				.type(Integer.class);

		return ret;
	}
}
