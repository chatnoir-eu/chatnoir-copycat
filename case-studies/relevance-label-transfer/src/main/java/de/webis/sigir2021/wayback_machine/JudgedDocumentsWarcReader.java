package de.webis.sigir2021.wayback_machine;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;

public class JudgedDocumentsWarcReader {

	private final Path dir;
	
	public JudgedDocumentsWarcReader(String dir) {
		this.dir = Paths.get(dir);
	}

	@SneakyThrows
	public String entries(int topic, String documentId) {
		Path warcFile = uniqueWARCFile(topic);
		WARCReader reader = WARCReaderFactory.get(warcFile.toFile());
		
		return recordWithId(reader, documentId);
	}

	private String recordWithId(WARCReader reader, String documentId) {
		for(ArchiveRecord record: reader) {
			if(documentId.equalsIgnoreCase((String) record.getHeader().getHeaderFields().get("trec_target_id"))) {
				return content(record);
			}
		}
		
		return null;
	}
	
	@SneakyThrows
	private static String content(ArchiveRecord record) {
		ByteArrayOutputStream ret = new ByteArrayOutputStream();
		record.dump(ret);
		
		return ret.toString(StandardCharsets.UTF_8.toString());
	}

	private Path uniqueWARCFile(int topic) {
		Path parentDir = dir.resolve("topic-" + topic);
		String[] entries = parentDir.toFile().list();
		
		if(entries == null || entries.length != 1) {
			throw new RuntimeException("Invalid dir: " + parentDir);
		}
		
		return parentDir.resolve(entries[0]);
	}

	@SneakyThrows
	public static Date parseDate(String line) {
		try {
			String ret = line.split("\\s+")[1];
			
			return sdf().parse(ret);
		} catch(Exception e) {
			throw new RuntimeException("'" + line + "'");
		}
	}
	
	public static String waybackUrl(String cdxRecord) {
		String date = cdxRecord.split("\\s+")[1];
		
		return "http://web.archive.org/web/" + date + "/" + targetUrl(cdxRecord);
	}
	
	public static String targetUrl(String cdxRecord) {
		return cdxRecord.split("\\s+")[2];
	}
	
	@SneakyThrows
	public static String resolveRedirect(String cdxRecord) {
		if(cdxRecord == null || parseResponseCode(cdxRecord) < 300 || parseResponseCode(cdxRecord)> 400) {
			return null;
		}
		
		return resolveRedirect(new URL(waybackUrl(cdxRecord)), 0);
	}
	
	@SneakyThrows
	public static String resolveRedirect(URL url, int depth) {
		if(depth > 4) {
			return url == null ? null : url.toString();
		}
		
		String html = htmlFailsafe(url.toString());
		String ret = extractRedirectMessageFromHtml(html);
		
		if (ret == null) {
			return url == null ? null : url.toString();
		} else {
			return resolveRedirect(new URL(ret), depth +1);
		}
	}
	
	@SneakyThrows
	private static String htmlFailsafe(String urlStr) {
		URL url = new URL(urlStr);
		return Failsafe.with(CdxApi.RETRY_POLICY).get(() -> html(url));
	}
	
	@SneakyThrows
	private static String html(URL url) {
		return IOUtils.toString(url);
	}

	private static SimpleDateFormat sdf() {
		SimpleDateFormat ret = new SimpleDateFormat("yyyyMMddHHmmss");
		ret.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
		
		return ret;
	}
	
	private static Date centerOfCw12Crawl() {
		return parseDate("a 20120401000000 a");
	}

	public static int parseResponseCode(String line) {
		try {
			return Integer.parseInt(line.split("\\s+")[4]);
		} catch(Exception e) {
			return 0;
		}
	}

	public static List<String> keepResponsesInCw12CrawlingTime(List<String> entries) {
		return entries.stream()
				.filter(i -> responseInCw12CrawlingTime(i))
				.collect(Collectors.toList());
	}

	public static boolean responseInCw12CrawlingTime(Date date) {
		return parseDate("a 20111100000000 b").getTime() <= date.getTime() && 
				parseDate("a 20120800000000 b").getTime() >= date.getTime();
	}
	
	private static boolean responseInCw12CrawlingTime(String l) {
		if(l == null || l.trim().isEmpty()) {
			return false;
		}
		
		return responseInCw12CrawlingTime(parseDate(l));
	}

	private static Long diffToCW12(String line) {
		return Math.abs(centerOfCw12Crawl().getTime() - parseDate(line).getTime());
	}

	public String bestMatchingCW12SnapshotOrNull(int topic, String documentId) {
		return bestMatchingCW12SnapshotOrNull(entries(topic, documentId));
	}
	
	public String bestMatchingCW12SnapshotOrNull(String entryString) {
		if(entryString == null) {
			return null;
		}
		
		List<String> entries = Arrays.asList(entryString.split("\n"));
		if(entries.size() == 0) {
			return null;
		}
		
		entries = keepResponsesInCw12CrawlingTime(entries);
		entries = new ArrayList<>(entries.stream().filter(i -> 200 == parseResponseCode(i)).collect(Collectors.toList()));
		if(entries.size() == 0) {
			return null;
		}
		
		Collections.sort(entries, (a,b) -> diffToCW12(a).compareTo(diffToCW12(b)));
		return entries.get(0);
	}

	public String bestMatchingCW12RedirectOrNull(int topic, String documentId) {
		String entryString = entries(topic, documentId);
		if(entryString == null || bestMatchingCW12SnapshotOrNull(entryString) != null) {
			return null;
		}		

		List<String> entries = Arrays.asList(entryString.split("\n"));
		if(entries.size() == 0) {
			return null;
		}
		
		entries = keepResponsesInCw12CrawlingTime(entries);
		entries = new ArrayList<>(entries.stream().filter(i -> parseResponseCode(i) >= 300 && parseResponseCode(i) < 400).collect(Collectors.toList()));
		if(entries.size() == 0) {
			return null;
		}
		
		Collections.sort(entries, (a,b) -> diffToCW12(a).compareTo(diffToCW12(b)));
		return entries.get(0);
	}

	public static String extractRedirectMessageFromHtml(String html) {
		if(html == null) {
			return null;
		}
		
		Document doc = Jsoup.parse(html);
		Elements anchors = doc.select("p[class=impatient]>a[href]");
		
		if(anchors == null || anchors.size() != 1) {
			return null;
		}
		
		return anchors.get(0).attr("href");
	}
}
