package de.webis.sigir2021;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.archive.io.warc.WARCWriterPoolSettings;
import org.archive.io.warc.WARCWriterPoolSettingsData;
import org.archive.uid.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.sigir2021.wayback_machine.CdxApi;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class App {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}
		
		List<String> judgedDocuments = JudgedDocuments.judgedDocuments(parsedArgs);
		WARCWriter writer = writer(parsedArgs);

		persistAllDocuments(judgedDocuments, writer);
	}
	
	@SneakyThrows
	private static void persistAllDocuments(List<String> documents, WARCWriter writer) {
		for(String doc: documents) {
			LOGGER.info("Load snapshots of: " + doc);
			String url = JudgedDocuments.urlOfDocument(doc);
			if(url == null) {
				LOGGER.info("Document is not in ChatNoir: " + doc);
				continue;
			}
			
			WARCRecordInfo record = CdxApi.warcRecordForUrlBetween2009And2013(doc, url);

			if(record != null) {
				writer.writeRecord(record);
			} else {
				LOGGER.info("Document " + doc + " can not be retrieved from " + url);
			}
		}
		
		LOGGER.info("Done!");
		writer.close();
	}

	static WARCWriter writer(Namespace parsedArgs) {
		AtomicInteger serialNo = new AtomicInteger();
		WARCWriterPoolSettings settings = warcSettings(parsedArgs);
		
		return new WARCWriter(serialNo, settings);
	}
	
	private static WARCWriterPoolSettings warcSettings(Namespace parsedArgs) {
		String topic = "topic-" + parsedArgs.getInt("topic");
		
		return new WARCWriterPoolSettingsData(
			topic +"-",
			"${prefix}-${timestamp17}-${serialno}",
			1024*1024*100,
			true,
			Arrays.asList(new File(parsedArgs.getString("output") + "/" + topic)),
			new ArrayList<>(),
			new UUIDGenerator()
		);
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
		ArgumentParser ret = ArgumentParsers.newFor("trec-judgments-in-wayback-machine")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-t", "--task")
				.required(Boolean.TRUE)
				.help("The shared task");

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
