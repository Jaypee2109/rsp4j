package org.streamreasoning.rsp4j.io.sources;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.WebDataStream;
import org.streamreasoning.rsp4j.io.WebDataStreamImpl;
import org.streamreasoning.rsp4j.io.utils.parsing.ParsingResult;
import org.streamreasoning.rsp4j.io.utils.parsing.ParsingStrategy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

/**
 * Source that pull HTTP (GET) access point periodically. Uses {@link ParsingStrategy} for creating of objects of type T.
 *
 * @param <T>  generic type of objects that populate the stream.
 */
public class HTTPPullSource<T> extends WebDataStreamImpl<T> {

    private final String url;
    private final long timeOut;
    private final ParsingStrategy<T> parsingStrategy;
    private volatile boolean streaming = false;

    /**
     * Creates a new HTTPPullSource
     * @param streamURI  the uri of the WebStream, this is only used for linking to the WebStream internally
     * @param url  the url of the http access point
     * @param timeout  the timeout in milliseconds between pulling the access point
     * @param parsingStrategy  the parsing strategy used for parsing the received strings to objects of type T
     */
    public HTTPPullSource(String streamURI, String url, long timeout, ParsingStrategy<T> parsingStrategy){
        this.stream_uri = streamURI;
        this.url = url;
        this.timeOut = timeout;
        this.parsingStrategy = parsingStrategy;

    }

    /**
     * Creates a new HTTPPullSource and using the url of the access point as {@code streamURI}
     * @param url  the url of the http access point, this url is used as streamURI
     * @param timeout  the timeout in milliseconds between pulling the access point
     * @param parsingStrategy  the parsing strategy used for parsing the received strings to objects of type T
     */
    public HTTPPullSource(String url, long timeout,ParsingStrategy<T> parsingStrategy){
        this(url,url,timeout,parsingStrategy);
    }

    /**
     * Start pulling the http access point and populating the stream.
     * The {@link ParsingStrategy} is used for converting the received strings to objects of type T
     */
    public void stream() {
        this.streaming=true;
        Runnable task = () -> {
        while (this.streaming) {
            URL urlCon;
            try {
                urlCon = new URL(url);
                URLConnection conn = urlCon.openConnection();
                InputStream is = conn.getInputStream();
                String result = "";
                try (BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset()))) {
                    result = br.lines().collect(Collectors.joining(System.lineSeparator()));
                    ParsingResult<T> parsingResult = parsingStrategy.parse(result);
                    this.put(parsingResult.getResult(),parsingResult.getTimeStamp());
                }
                Thread.sleep(this.timeOut);
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        };
        Thread thread = new Thread(task);
        thread.start();
    }

    /**
     * Stops the stream
     */
    public void stop(){
        this.streaming = false;
    }
}
