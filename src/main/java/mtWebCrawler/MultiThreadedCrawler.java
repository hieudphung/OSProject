package mtWebCrawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class MultiThreadedCrawler {
    private final String seedUrl;
    private final String rootUrl;
    private final ExecutorService pool;
    private final Set<String> scrapedPages = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<String> crawlQueue = new LinkedBlockingQueue<>();
    private final String resumeFile;

    public MultiThreadedCrawler(String seedUrl, int maxWorkers) {
        this.seedUrl = seedUrl;
        this.rootUrl = URLUtil.getRootUrl(seedUrl);
        this.pool = Executors.newFixedThreadPool(maxWorkers);

        // Create state file name based on domain, excluding the TLD
        this.resumeFile = URLUtil.getDomainWithoutTld(seedUrl) + "_crawler_state.json";

        // Load state if resuming
        loadState();

        // Add shutdown hook to save state in case of unexpected termination
        Runtime.getRuntime().addShutdownHook(new Thread(this::saveState));
    }

    private void loadState() {
        if (new File(resumeFile).exists()) {
            try (Reader reader = new FileReader(resumeFile)) {
                State state = new Gson().fromJson(reader, State.class);
                Collections.addAll(scrapedPages, state.scrapedPages);
                for (String url : state.queue) {
                    if (!scrapedPages.contains(url)) {
                        crawlQueue.add(url);
                    }
                }
            } catch (IOException e) {
                System.err.println("Failed to load state: " + e.getMessage());
            }
        } else {
            crawlQueue.add(seedUrl);
        }
    }

    private void saveState() {
        State state = new State(scrapedPages.toArray(new String[0]), crawlQueue.toArray(new String[0]));
        try (Writer writer = new FileWriter(resumeFile)) {
            new GsonBuilder().setPrettyPrinting().create().toJson(state, writer);
        } catch (IOException e) {
            System.err.println("Failed to save state: " + e.getMessage());
        }
    }

    private void parseLinks(String html) {
        Document doc = Jsoup.parse(html);
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            String url = link.absUrl("href");
            if ((url.startsWith("https://")) && (url.startsWith(rootUrl) || url.startsWith("/"))) {
                if (!scrapedPages.contains(url)) {
                    crawlQueue.add(url);
                    System.out.println("Discovered link: " + url);
                }
            }
        }
    }

    private void scrapeInfo(String html) {
        Document doc = Jsoup.parse(html);
        Elements paragraphs = doc.select("p");
        StringBuilder text = new StringBuilder();
        for (Element para : paragraphs) {
            if (!para.text().contains("https:")) {
                text.append(para.text().trim()).append("\n");
            }
        }
        System.out.println("\n<--- Text Present in The WebPage is --->\n" + text.toString());
    }

    private void scrapePage(String url) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(3000);
            connection.setReadTimeout(30000);
            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                String html = new String(connection.getInputStream().readAllBytes());
                parseLinks(html);
                scrapeInfo(html);
            }
        } catch (IOException e) {
            System.err.println("Failed to retrieve " + url + ": " + e.getMessage());
        }
    }

    public void runWebCrawler() {
        try {
            while (!crawlQueue.isEmpty()) {
                String targetUrl = crawlQueue.poll(60, TimeUnit.SECONDS);
                if (targetUrl != null && !scrapedPages.contains(targetUrl)) {
                    System.out.println("Scraping URL: " + targetUrl);
                    scrapedPages.add(targetUrl);
                    pool.submit(() -> scrapePage(targetUrl));
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Crawler interrupted: " + e.getMessage());
        } finally {
            pool.shutdown();
            saveState();
            System.out.println("Crawler stopped and state saved.");
        }
    }

    public void info() {
        System.out.println("\nSeed URL is: " + seedUrl);
        System.out.println("Scraped pages: " + scrapedPages);
    }

    private static class State {
        String[] scrapedPages;
        String[] queue;

        public State(String[] scrapedPages, String[] queue) {
            this.scrapedPages = scrapedPages;
            this.queue = queue;
        }
    }

    private static class URLUtil {
        static String getRootUrl(String url) {
            return url.split("/")[0] + "//" + url.split("/")[2];
        }

        static String getDomainWithoutTld(String url) {
            String domain = url.split("/")[2];
            String[] domainParts = domain.split("\\.");
            return String.join("_", Arrays.copyOf(domainParts, domainParts.length - 1));
        }
    }

    public static void main(String[] args) {
        String seedUrl = "https://abc.com/";
        int maxThreads = 5;
        MultiThreadedCrawler crawler = new MultiThreadedCrawler(seedUrl, maxThreads);
        crawler.runWebCrawler();
        crawler.info();
    }
}
