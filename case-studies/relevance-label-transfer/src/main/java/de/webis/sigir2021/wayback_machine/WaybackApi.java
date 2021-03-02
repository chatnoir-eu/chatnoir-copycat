package de.webis.sigir2021.wayback_machine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.archive.format.warc.WARCConstants;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.uid.UUIDGenerator;
import org.archive.util.anvl.ANVLRecord;

import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class WaybackApi {

	public static final RetryPolicy<Pair<ByteArrayOutputStream, Map<String, String>>> RETRY_FINAL = new RetryPolicy<Pair<ByteArrayOutputStream, Map<String, String>>>()
			.handle(Exception.class)
			.withBackoff(3, 100, ChronoUnit.SECONDS).withMaxRetries(4);
	
	
	@SneakyThrows
	public static WARCRecordInfo warcRecord(String id, String cdxLine) {
		Pair<ByteArrayOutputStream, Map<String, String>> response = contentFailsave(cdxLine);
		Map<String, String> headers = response.getRight();
		WARCRecordInfo ret = new WARCRecordInfo();
		ret.setType(WARCRecordType.response);
		ret.setRecordId(new UUIDGenerator().getRecordID());
		InputStream contentStream = new ByteArrayInputStream(response.getLeft().toByteArray());
		
		ret.setContentLength(Integer.parseInt(headers.get("Content-Length")));
		ret.setMimetype(headers.get("Content-Type"));
		
		ANVLRecord extraHeaders = new ANVLRecord();
		extraHeaders.addLabelValue(WARCConstants.HEADER_KEY_URI, JudgedDocumentsWarcReader.waybackUrl(cdxLine));
		extraHeaders.addLabelValue("trec_target_id", id);
		extraHeaders.addLabelValue("trec_target_uri", JudgedDocumentsWarcReader.targetUrl(cdxLine));
		for(String header: headers.keySet()) {
			if(header == null || header.equalsIgnoreCase("Content-Length") || header.equalsIgnoreCase("Content-Type")) {
				continue;
			}
			
			extraHeaders.addLabelValue(header, headers.get(header));
		}
		
		ret.setExtraHeaders(extraHeaders);
		
		ret.setContentStream(contentStream);
		
		return ret;
	}
	
	@SneakyThrows
	public static WARCRecordInfo warcRecord(String id, URL url) {
		Pair<ByteArrayOutputStream, Map<String, String>> response = contentFailsave(url);
		Map<String, String> headers = response.getRight();
		WARCRecordInfo ret = new WARCRecordInfo();
		ret.setType(WARCRecordType.response);
		ret.setRecordId(new UUIDGenerator().getRecordID());
		InputStream contentStream = new ByteArrayInputStream(response.getLeft().toByteArray());
		
		ret.setContentLength(Integer.parseInt(headers.get("Content-Length")));
		ret.setMimetype(headers.get("Content-Type"));
		
		ANVLRecord extraHeaders = new ANVLRecord();
		extraHeaders.addLabelValue(WARCConstants.HEADER_KEY_URI, url.toString());
		extraHeaders.addLabelValue("trec_target_id", id);
		extraHeaders.addLabelValue("trec_target_uri", url.toString());
		for(String header: headers.keySet()) {
			if(header == null || header.equalsIgnoreCase("Content-Length") || header.equalsIgnoreCase("Content-Type")) {
				continue;
			}
			
			extraHeaders.addLabelValue(header, headers.get(header));
		}
		
		ret.setExtraHeaders(extraHeaders);
		
		ret.setContentStream(contentStream);
		
		return ret;
	}
	
	@SneakyThrows
	private static Pair<ByteArrayOutputStream, Map<String, String>> contentFailsave(String cdxLine) {
		URL waybackUrl = new URL(JudgedDocumentsWarcReader.waybackUrl(cdxLine));
		return contentFailsave(waybackUrl);
	}
	
	private static Pair<ByteArrayOutputStream, Map<String, String>> contentFailsave(URL waybackUrl) {
		return Failsafe.with(RETRY_FINAL).get(() -> content(waybackUrl));
	}
	
	@SneakyThrows
	private static Pair<ByteArrayOutputStream, Map<String, String>> content(URL waybackUrl) {
		
		URLConnection conn = waybackUrl.openConnection();
		
		ByteArrayOutputStream ret = new ByteArrayOutputStream();
		IOUtils.copy(conn.getInputStream(), ret);
		
		Map<String, String> headers = new LinkedHashMap<>();
		for(String header: conn.getHeaderFields().keySet()) {
			headers.put(header, conn.getHeaderField(header));
		}
		
		return Pair.of(ret, headers);
	}
}
