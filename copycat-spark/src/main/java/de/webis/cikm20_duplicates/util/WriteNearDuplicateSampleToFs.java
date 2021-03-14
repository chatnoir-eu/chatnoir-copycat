package de.webis.cikm20_duplicates.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import de.webis.cikm20_duplicates.app.SampleNearDuplicates.NdSample;
import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels.CanonicalLinkGraphEdge2;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.EsDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class WriteNearDuplicateSampleToFs {
	private static final String
	DIR = "src/main/resources/near-duplicate-sample/cc-2017-04/";
	
	//CREATED BY: de.webis.cikm20_duplicates.app.SampleNearDuplicatesIntegration
	private static final Path INPUT_PATH = Paths.get("src/main/resources/identified-near-duplicates-human-judgments/cw09.jsonl");
	
	
//	FIXME: Kann ich mittels S3 die kompletten near-duplicates berechnen, indem ich die Kandidaten final abteste?
//			Z.B. indem ich auf basis der near-duplicates geeignete blöcke bilde, die dann mit S3-Index getestet werden können?
//			Irgendwie in der Art: Die near-duplicate-Pairs geben mir irgendwie gruppen vor, mit denen ich nochmal nachberechnen kann.
//			Vielleicht über irgendwas transitives?
//			Mit neuen Berechnungen?
//			- für k=0 ist das machbar-> partitioniere Dokumente in den jeweiligen Block
//			- für k=1 muss man sich alle gruppen speichern, in die es verglichen werden muss?
//			  - Kann ich vielleicht die Idee anwenden, die in dem anderen Gedanken (bilde immer ab auf minimum) angewandt wurden?
//					  
//	- Nutze Elasticsearch fuer wahlfreien Zugriff auf Dokumente zur Pruefung
	public static void main(String[] args) {
		int count = 0;
		int wrong = 0;
		for(NdSample sample: samples(INPUT_PATH)) {
			CollectionDocument a = new EsDocumentResolver().loadCollectionDocument(sample.getFirstId());
			CollectionDocument b = new EsDocumentResolver().loadCollectionDocument(sample.getSecondId());
			double tmpScore = -1;
			if(a != null && b != null) {
				tmpScore = SparkEnrichRelevanceTransferPairs.s3Score(a,b);
			}
			
			System.out.println("hemming: " + sample.getHemmingDistance() +"; s3Score: " + sample.getS3Score() +"; mainContentS3Score: " + tmpScore);
			
			if(sample.getS3Score() < 0.82) {
//				System.out.println(sample.getS3Score() + " --> " + sample.getHemmingDistance());
			}
			
			int minHemming = 0;
			if(minHemming >= sample.getHemmingDistance()) {
				count++;
			}
			
			if(sample.getS3Score() < 0.82) {
//				System.out.println(sample.getFirstURL() + " vs " + sample.getSecondURL());
//				System.out.println(sample.getS3Score() + " --> " + sample.getHemmingDistance());
//				System.out.println();
				
//				System.out.println(sample);
				if(minHemming >= sample.getHemmingDistance()) {
					wrong++;
				}
			}
//			System.out.println(sample.getS3Score());
//			persistExample(sample)
		}
		
		System.out.println("---> " + (((double) wrong)/(double) count));
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
