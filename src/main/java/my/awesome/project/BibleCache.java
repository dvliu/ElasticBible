package my.awesome.project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Slf4j
public class BibleCache {

	static ObjectMapper mapper = new ObjectMapper();
	static Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

	public static void main(String[] args) throws Throwable {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("META-INF/spring/app-context.xml");
		BibleCache bCache = context.getBean(BibleCache.class);
		context.registerShutdownHook();

		bCache.run("C:/playground/play-workspace/ElasticBible/src/main/resources/bibletext");
	}

	public void run(String path) {
		try {
			// TODO: time how long the process takes
			File[] files = new File(path).listFiles();
			for (File file : files) {
				createIndexWithHash(parseTxtFilesToHash(file.getAbsolutePath().replace("\\", "/")));
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> parseTxtFilesToHash(String path) {
		LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> bookToChapterMap = Maps.newLinkedHashMap();
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			File outFile = new File("C:/playground/test_output/outFile+" + df.format(new Date()) + ".txt");
			// ClassPathResource resource = new ClassPathResource(path);
			// BufferedReader reader = new BufferedReader(new
			// InputStreamReader(new
			// DataInputStream(resource.getInputStream())));
			BufferedReader reader = new BufferedReader(new FileReader(path));
			int count = 0;
			String currentLine;
			String book = "";
			String chapter = "";
			LinkedHashMap<String, LinkedHashMap<String, String>> chapterToVerseMap = Maps.newLinkedHashMap();
			while ((currentLine = reader.readLine()) != null) {
				if (currentLine.isEmpty()) {
					continue;
				}
				if (count == 0) {
					book = currentLine.replaceAll(" ", "_").toLowerCase();
					count++;
					continue;
				}
				while (currentLine.contains("CHAPTER")) {
					chapter = currentLine.replaceFirst("CHAPTER ", "");
					LinkedHashMap<String, String> verseMap = Maps.newLinkedHashMap();
					String verseLine;
					while ((verseLine = reader.readLine()) != null) {
						if (verseLine.contains("CHAPTER") || verseLine.isEmpty()) {
							break;
						}
						Pattern versePattern = Pattern.compile("-?\\d+");
						Matcher verseMatcher = versePattern.matcher(verseLine);
						int num = 0;
						String verse = "";
						String text = "";
						while (verseMatcher.find() && num == 0) {
							num++;
							verse = verseMatcher.group();
							text = verseLine.replaceFirst(verse + " ", "");
						}
						verseMap.put(verse, text);
					}
					chapterToVerseMap.put(chapter, verseMap);
					break;
				}
				bookToChapterMap.put(book, chapterToVerseMap);
			}
			reader.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return bookToChapterMap;
	}

	public String parseTxtFilesToString(String path) {
		String answer = "";
		LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> bookToChapterMap = Maps.newLinkedHashMap();
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			File outFile = new File("C:/playground/test_output/outFile+" + df.format(new Date()) + ".txt");
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
			// ClassPathResource resource = new ClassPathResource(path);
			// BufferedReader reader = new BufferedReader(new InputStreamReader(new
			// DataInputStream(resource.getInputStream())));
			BufferedReader reader = new BufferedReader(new FileReader(path));
			int count = 0;
			String currentLine;
			String book = "";
			String chapter = "";
			LinkedHashMap<String, LinkedHashMap<String, String>> chapterToVerseMap = Maps.newLinkedHashMap();
			while ((currentLine = reader.readLine()) != null) {
				if (currentLine.isEmpty()) {
					continue;
				}
				if (count == 0) {
					book = currentLine.replaceAll(" ", "_").toLowerCase();
					count++;
					continue;
				}
				while (currentLine.contains("CHAPTER")) {
					chapter = currentLine.replaceFirst("CHAPTER ", "");
					LinkedHashMap<String, String> verseMap = Maps.newLinkedHashMap();
					String verseLine;
					while ((verseLine = reader.readLine()) != null) {
						if (verseLine.contains("CHAPTER") || verseLine.isEmpty()) {
							break;
						}
						Pattern versePattern = Pattern.compile("-?\\d+");
						Matcher verseMatcher = versePattern.matcher(verseLine);
						int num = 0;
						String verse = "";
						String text = "";
						while (verseMatcher.find() && num == 0) {
							num++;
							verse = verseMatcher.group();
							text = verseLine.replaceFirst(verse + " ", "");
						}
						verseMap.put(verse, text);
					}
					chapterToVerseMap.put(chapter, verseMap);
					break;
				}
				bookToChapterMap.put(book, chapterToVerseMap);
			}
			answer = mapper.writeValueAsString(bookToChapterMap);
			log.info(answer);
			out.print(answer);
			reader.close();
			out.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return answer;
	}

	public void createIndexWithString(String json) {
		try {
			JsonNode rootNode = mapper.readTree(json);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public void createIndexWithHash(LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> hash) {
		try {
			Iterator<String> books = hash.keySet().iterator();
			String book = "";
			int num = 0;
			while (books.hasNext() && num == 0) {
				book = books.next();
				num++;
			}
			for (LinkedHashMap<String, LinkedHashMap<String, String>> chapters : hash.values()) {
				for (Entry<String, LinkedHashMap<String, String>> chapterSet : chapters.entrySet()) {
					String chapter = chapterSet.getKey();
					LinkedHashMap<String, String> verses = chapterSet.getValue();
					IndexResponse response = client.prepareIndex(book, chapter).setSource(mapper.writeValueAsString(verses)).execute().actionGet();
					log.debug(response.toString());
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
}
