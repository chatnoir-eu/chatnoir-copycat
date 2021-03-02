package de.webis.sigir2021;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import lombok.SneakyThrows;

public class S3Aggregation {

	@SneakyThrows
	public static void main(String[] args) {
		Files.write(Paths.get("src/main/resources/s3-aggregations-cw09-to-cw12.json"), countS3SimilaritiesCw09ToCw12(Arrays.asList(
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2009.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2010.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2011.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2012.jsonl")
		)).getBytes());
		
		Files.write(Paths.get("src/main/resources/s3-aggregations-cw09-to-cw12-and-wayback.json"), countS3SimilaritiesCw09ToCw12AndWayback(Arrays.asList(
				new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2009.jsonl"),
				new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2010.jsonl"),
				new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2011.jsonl"),
				new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/web-2012.jsonl")
			), new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/wayback-similarities.jsonl")).getBytes());
		
		Files.write(Paths.get("src/main/resources/s3-aggregations-cw-to-cc15.json"), countS3SimilaritiesCc15(Arrays.asList(
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2009.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2010.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2011.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2012.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2013.jsonl"),
			new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/cc15-relevance-transfer/web-2014.jsonl")
		)).getBytes());
	}
	
	@SneakyThrows
	public static String countS3SimilaritiesCw09ToCw12(List<File> input) {
		return new ObjectMapper().writeValueAsString(internalCountS3SimilaritiesCw09ToCw12(input));
	}
	
	@SneakyThrows
	private static Map<String, Integer> internalCountS3SimilaritiesCw09ToCw12(List<File> input) {
		Map<String, Integer> bla = ret();
		for(File f: input) {
			List<String> lines = Files.readAllLines(f.toPath());
			for(String line: lines) {
				Double score = cw09ToCW12ScoreOrNull(line);
				
				if(score != null) {
					String bucket = bucket(score);
					bla.put(bucket, 1+ bla.getOrDefault(bucket, 0));
				}
			}
		}

		return bla;
	}
	
	@SneakyThrows
	public static String countS3SimilaritiesCc15(List<File> input) {
		Map<String, Integer> bla = ret();
		for(File f: input) {
			List<String> lines = Files.readAllLines(f.toPath());
			for(String line: lines) {
				Double score = cc15ScoreOrNull(line);
				
				if(score != null) {
					String bucket = bucket(score);
					bla.put(bucket, 1+ bla.getOrDefault(bucket, 0));
				}
			}
		}

		return new ObjectMapper().writeValueAsString(bla);
	}

	@SneakyThrows
	public static String countS3SimilaritiesCw09ToCw12AndWayback(List<File> input, File waybackFile) {
		Map<String, Integer> ret = internalCountS3SimilaritiesCw09ToCw12(input);
		List<String> lines = Files.readAllLines(waybackFile.toPath());
		for(String line: lines) {
			JSONObject object = new JSONObject(line);
			Double score = object.getDouble("s3Score");
			
			if(score != null) {
				String bucket = bucket(score);
				ret.put(bucket, 1+ ret.getOrDefault(bucket, 0));
			}
		}
		
		return new ObjectMapper().writeValueAsString(ret);
	}

	private static Map<String, Integer> ret() {
		Map<String, Integer> ret = new LinkedHashMap<>();
		
		for(String bucket: sortedBuckets()) {
			ret.put(bucket, 0);
		}
		
		return ret;
	}
	
	private static Double cw09ToCW12ScoreOrNull(String line) {
		JSONObject json = new JSONObject(line);
		int cw12Matches = json.getInt("cw12Matches");
		
		if(cw12Matches > 0) {
			return json.getDouble("cw12UrlMaxS3Score");
		} else {
			return null;
		}
	}
	
	private static Double cc15ScoreOrNull(String line) {
		JSONObject json = new JSONObject(line);
		int urlMatches = json.getInt("urlMatches");
		
		if(urlMatches > 0) {
			return json.getDouble("urlMaxS3Score");
		} else {
			return null;
		}
	}

	public static String bucket(double score) {
		List<String> buckets = sortedBuckets();
		String ret = buckets.get(0);
		for(String tmp: buckets) {
			if(Double.valueOf(tmp) <= score) {
				ret = tmp;
			} else if (score > Double.valueOf(tmp)) {
				break;
			}
		}
		
		return ret;
	}
	
	private static List<String> sortedBuckets() {
		List<String> ret = new ArrayList<>();
		BigDecimal stepSize = new BigDecimal("0.0050");
		BigDecimal current = BigDecimal.ZERO;
		
		while(current.compareTo(BigDecimal.ONE) < 0) {
			ret.add(new DecimalFormat("#0.0000").format(current));
			current = current.add(stepSize);
		}
		
		return ret;
	}
}
