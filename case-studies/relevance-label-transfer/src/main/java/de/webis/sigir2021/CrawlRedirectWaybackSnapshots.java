package de.webis.sigir2021;

import java.net.URL;
import java.util.List;

import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;
import de.webis.sigir2021.wayback_machine.WaybackApi;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.inf.Namespace;

public class CrawlRedirectWaybackSnapshots {
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlRedirectWaybackSnapshots.class);
	
	public static void main(String[] args) {
		Namespace parsedArgs = CrawlWaybackSnapshots.validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}
		
		List<String> judgedDocuments = JudgedDocuments.judgedDocuments(parsedArgs);
		JudgedDocumentsWarcReader judgedDocWarcReader = new JudgedDocumentsWarcReader(parsedArgs.getString("inputWarc"));
		WARCWriter writer = App.writer(parsedArgs);
		
		LOGGER.info("Load redirects of topic " + parsedArgs.getInt("topic"));
		crawlRedirectSnapshots(judgedDocuments, writer, parsedArgs.getInt("topic"), judgedDocWarcReader);
	}
	
	@SneakyThrows
	private static void crawlRedirectSnapshots(List<String> judgedDocuments, WARCWriter writer, int topic, JudgedDocumentsWarcReader judgedDocWarcReader) {
		for(String documentId: judgedDocuments) {
			try {
				String bestMatchingRedirectUrl = JudgedDocumentsWarcReader.resolveRedirect(judgedDocWarcReader.bestMatchingCW12RedirectOrNull(topic, documentId));
				
				if(bestMatchingRedirectUrl == null) {
					continue;
				}
				
				LOGGER.info("Load redirect: " + bestMatchingRedirectUrl);
				
				WARCRecordInfo record = WaybackApi.warcRecord(documentId, new URL(bestMatchingRedirectUrl));
				writer.writeRecord(record);
			} catch(Exception e) {
				LOGGER.warn("Could not load readirect: " + judgedDocWarcReader.bestMatchingCW12RedirectOrNull(topic, documentId));
			}
		}
		
		writer.close();
	}
}
