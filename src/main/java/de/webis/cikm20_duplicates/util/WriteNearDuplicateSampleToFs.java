package de.webis.cikm20_duplicates.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import de.webis.cikm20_duplicates.app.SampleNearDuplicates.NdSample;
import de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels.CanonicalLinkGraphEdge2;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class WriteNearDuplicateSampleToFs {
	private static final String
	DIR = "src/main/resources/near-duplicate-sample/cc-2017-04/";
	
	//CREATED BY: de.webis.cikm20_duplicates.app.SampleNearDuplicatesIntegration
	private static final Path INPUT_PATH = Paths.get("src/main/resources/identified-near-duplicates-human-judgments/cc-2017-04.jsonl");
	
	public static void main(String[] args) {
		for(NdSample sample: samples(INPUT_PATH)) {
			if(sample.getS3Score() < 0.82) {
//				System.out.println(sample.getS3Score() + " --> " + sample.getHemmingDistance());
			}
			
			if(sample.getS3Score() < 0.01) {
				System.out.println(sample.getFirstURL() +" vs " + sample.getSecondURL());
//				System.out.println(sample);
			}
//			System.out.println(sample.getS3Score());
//			persistExample(sample)
		}
	}
	
	@SneakyThrows
	private static void persistExample(NdSample sample) {
		CollectionDocument src = sample.getFirstDocument();
		CollectionDocument target = sample.getSecondDocument();
		
		Path dir = Paths.get(DIR + src.getId() + "__to__" + target.getId());
		dir.toFile().mkdirs();
		
		Files.write(dir.resolve("metadata"), metadata(sample).getBytes(StandardCharsets.UTF_8));
		Files.write(dir.resolve("src-doc"), src.getFullyCanonicalizedContent().getBytes(StandardCharsets.UTF_8));
		Files.write(dir.resolve("target-doc"), target.getFullyCanonicalizedContent().getBytes(StandardCharsets.UTF_8));
	}
	
	private static String metadata(NdSample sample) {
		return "First: "+ sample.getFirstURL() +"\n" + 
				"Second: "+ sample.getSecondId() +"\n" +
				"S3: " + sample.getS3Score() +"\n";
	}
	
	@SneakyThrows
	private static List<NdSample> samples(Path path) {
		List<String> lines = Files.readAllLines(path);
		List<NdSample> ret = new ArrayList<>();
		
		for(String line: lines) {
			ret.add(NdSample.fromString(line));
		}
		
		return ret;
	}
}
