package de.webis.sigir2021;

import java.util.List;

import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;
import de.webis.sigir2021.wayback_machine.WaybackApi;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CrawlWaybackSnapshots {
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlWaybackSnapshots.class);

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}
		
		List<String> judgedDocuments = JudgedDocuments.judgedDocuments(parsedArgs);
		JudgedDocumentsWarcReader judgedDocWarcReader = new JudgedDocumentsWarcReader(parsedArgs.getString("inputWarc"));
		WARCWriter writer = App.writer(parsedArgs);

		crawlAllSnapshots(judgedDocuments, writer, parsedArgs.getInt("topic"), judgedDocWarcReader);
	}
	
	@SneakyThrows
	private static void crawlAllSnapshots(List<String> judgedDocuments, WARCWriter writer, int topic, JudgedDocumentsWarcReader judgedDocWarcReader) {
		for(String documentId: judgedDocuments) {
			String cdxLine = judgedDocWarcReader.bestMatchingCW12SnapshotOrNull(topic, documentId);
			if(cdxLine == null) {
				continue;
			}
			
			LOGGER.info("Load snapshot: " + JudgedDocumentsWarcReader.waybackUrl(cdxLine));
			try {
				WARCRecordInfo record = WaybackApi.warcRecord(documentId, cdxLine);
				writer.writeRecord(record);
			} catch(Exception e) {
				LOGGER.warn("Could not load snapshot: " + JudgedDocumentsWarcReader.waybackUrl(cdxLine));
			}
		}
		
		writer.close();
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
		ArgumentParser ret = ArgumentParsers.newFor("crawl-judged-snapshots-from-wayback-machine")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-t", "--task")
				.required(Boolean.TRUE)
				.help("The shared task");

		ret.addArgument("-i", "--inputWarc")
				.required(Boolean.TRUE)
				.help("The directory that contains all warc files with cdx informations (created with trec-judgments-in-wayback-machine)");
		
		ret.addArgument("-o", "--output")
				.required(Boolean.TRUE)
				.help("produce a warc file under this location.");

		ret.addArgument("--topic")
				.required(Boolean.TRUE)
				.type(Integer.class)
				.help("The topic number.");

		return ret;
	}
}
