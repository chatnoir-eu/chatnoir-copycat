package de.webis.copycat_cli;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.io.Files;

import lombok.SneakyThrows;
import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.spark.RunResultDeduplicator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class RunDeduplicationApp {
	private static final String ARG_INPUT_FILE = "inputRunFile";
	private static final String ARG_OUTPUT_FILE = "outputRunFile";
	private static final String ARG_DUPLICATE_FILE = "nearDuplicates";
	
	public static void main(String[] args) {
		deduplicateRun(args);
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
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Deduplication of run files and qrels.")
				.build();
		
		ret.addArgument("--" + ARG_INPUT_FILE)
			.required(true);

		ret.addArgument("--" + ARG_OUTPUT_FILE)
			.required(true);

		ret.addArgument("--" + ARG_DUPLICATE_FILE)
			.required(true);
		
		return ret;
	}

	public static void deduplicateRun(String[] args) {
		deduplicateRun(parseArgs(args));
	}
	
	@SneakyThrows
	private static void deduplicateRun(Namespace args) {
		List<DocumentGroup> documentGroups = DocumentGroup.readFromJonLines(Paths.get(args.getString(ARG_DUPLICATE_FILE)));
		List<RunLine> input = RunLine.parseRunlines(Paths.get(args.getString(ARG_INPUT_FILE)));
		List<RunLine> outRun = RunResultDeduplicator.removeDuplicates.deduplicateRun(input, documentGroups).getDeduplicatedRun();

		outRun = removeDuplicateIdsPerTopic(outRun);
		String out = outRun.stream().map(i -> i.toString()).collect(Collectors.joining("\n"));
		
		
		Files.write(out.getBytes(), Paths.get(args.getString(ARG_OUTPUT_FILE)).toFile());
	}
	
	private static List<RunLine> removeDuplicateIdsPerTopic(List<RunLine> run) {
		List<RunLine> ret = new ArrayList<>();
		Map<Integer, Set<String>> topicToDocs = new HashMap<>();
		for(RunLine line: run) {
			if(!topicToDocs.containsKey(line.getTopic())) {
				topicToDocs.put(line.getTopic(), new HashSet<>());
			}
			
			Set<String> docs = topicToDocs.get(line.getTopic());
			if(!docs.contains(line.getDoucmentID())) {
				ret.add(line);
			}
			
			docs.add(line.getDoucmentID());
		}
		
		return ret;
	}
}
