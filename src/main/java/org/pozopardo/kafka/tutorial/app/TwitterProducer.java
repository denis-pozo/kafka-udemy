package org.pozopardo.kafka.tutorial.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONFIG_FILE = "configuration.properties";
    private static final String bootstrapServer = "127.0.0.1:9092";

    private final Properties properties;
    private final String apiKey;
    private final String apiKeySecret;
    private final String accessToken;
    private final String accessTokenSecret;

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1_000);

    public TwitterProducer() {
        try {
            properties = loadProperties(CONFIG_FILE);
            apiKey = properties.getProperty("API_KEY");
            apiKeySecret = properties.getProperty("API_KEY_SECRET");
            accessToken = properties.getProperty("ACCESS_TOKEN");
            accessTokenSecret = properties.getProperty("ACCESS_TOKEN_SECRET");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        logger.info("Starting twitter producer");
        new TwitterProducer().run();
    }

    public void run() {
        Client twitterClient = createTwitterClient();
        KafkaProducer<String, String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> shutDownApplication(twitterClient, producer)));

        twitterClient.connect();
        while(!twitterClient.isDone()) { String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                shutDownApplication(twitterClient, producer);
            }
            if(msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if( e != null) {
                            logger.error("Ups, something bad happened", e);
                        }
                    }
                });
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private Client createTwitterClient() {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication auth = new OAuth1(apiKey, apiKeySecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("hosebird-Client-01")
                .hosts(Constants.STREAM_HOST)
                .authentication(auth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private void shutDownApplication(Client twitterClient, Producer kafkaProducer) {
        logger.info("closing twitter producer...");
        twitterClient.stop();
        logger.info("closing kafka producer...");
        kafkaProducer.flush();
        kafkaProducer.close();
        logger.info("shut down application...");
    }

    private static Properties loadProperties(String resourceFileName) throws IOException {
        Properties configuration = new Properties();
        InputStream input = TwitterProducer.class
                              .getClassLoader()
                              .getResourceAsStream(resourceFileName);
        configuration.load(input);
        return configuration;
    }

}
