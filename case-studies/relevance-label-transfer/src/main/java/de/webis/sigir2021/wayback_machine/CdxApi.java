package de.webis.sigir2021.wayback_machine;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.uid.UUIDGenerator;
import org.archive.util.anvl.ANVLRecord;

import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class CdxApi {
	public static final RetryPolicy<String> RETRY_POLICY = new RetryPolicy<String>()
			  .handle(Exception.class)
			  .withBackoff(10, 240, ChronoUnit.SECONDS)
			  .withMaxRetries(3);
	
	public static WARCRecordInfo warcRecordForUrlBetween2009And2013(String id, String url) {
		try {
			return warcRecordForUrlBetween2009And2013ThrowsException(id, url);
		} catch(Exception e) {
			return null;
		}
	}
	
	private static WARCRecordInfo warcRecordForUrlBetween2009And2013ThrowsException(String id, String url) {
		String content = snapshotsForUrlBetween2009And2013(url);
		
		WARCRecordInfo ret = new WARCRecordInfo();
		ret.setType(WARCRecordType.response);
		ret.setRecordId(new UUIDGenerator().getRecordID());
		ANVLRecord extraHeaders = new ANVLRecord();
		extraHeaders.addLabelValue(WARCConstants.HEADER_KEY_URI, cdxRequest(url, "2009", "2013").toString());
		extraHeaders.addLabelValue("trec_target_id", id);
		extraHeaders.addLabelValue("trec_target_uri", url);
		ret.setExtraHeaders(extraHeaders);
		InputStream contentStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
		ret.setContentLength(content.getBytes(StandardCharsets.UTF_8).length);
		ret.setMimetype("text/plain;charset=UTF-8");
		
		ret.setContentStream(contentStream);
		
		return ret;
	}
	
	public static String snapshotsForUrlBetween2009And2013(String url) {
		URL requestURL = cdxRequest(url, "2009", "2013");

		return snapshotsForUrlFailsave(requestURL);
	}

	private static String snapshotsForUrlFailsave(URL waybackUrl) {
		return snapshotsForUrlFailsave(waybackUrl, RETRY_POLICY);
	}
	
	private static String snapshotsForUrlFailsave(URL waybackUrl, RetryPolicy<String> retryPolicy) {
		return Failsafe.with(retryPolicy).get(() -> snapshotsForUrl(waybackUrl));
	}
	
	@SneakyThrows
	private static String snapshotsForUrl(URL waybackUrl) {
		return IOUtils.toString(waybackUrl, StandardCharsets.UTF_8);
	}
	
	@SneakyThrows
	private static URL cdxRequest(String url, String from, String to) {
		if(url.startsWith("https://")) {
			url = StringUtils.replaceFirst(url, "https://", "");
		}
		if(url.startsWith("http://")) {
			url = StringUtils.replaceFirst(url, "http://", "");
		}
		
		url = URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
		
		return new URL("http://web.archive.org/cdx/search/cdx?url=" + url + "&from=" + from + "&to=" + to);
	}
}
