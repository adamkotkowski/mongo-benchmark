import com.google.common.base.Stopwatch;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MongoTest {
    private static final String DB_URL = "mongodb://localhost:27017";
    private static final String DB_NAME = "test";
    private static final String COLLECTION_NAME = "w0j0";

    private static final int INSERTS_COUNT = 1000000; // number of inserts within one test
    private static final int TESTS_COUNT = 1; // number of independent tests to be performed (results are averages)
    private static final long SPEED_UNIT = 1000; // indicates the unit of speed - X ms/{amount} inserts
    private static final int NUMBER_OF_THREADS = 1;
    private static final int W = 1; // write concern setting

    public static final Logger logger = LoggerFactory.getLogger(MongoTest.class);

    public static void main(String[] args) {
        logger.info("Connecting to DB");
        final MongoClient mongoClient = new MongoClient(new MongoClientURI(DB_URL));
        mongoClient.getMongoClientOptions().getWriteConcern().withW(W);
        final MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        double result = 0;
        for (int i = 0; i < TESTS_COUNT; i++) {
            collection.deleteMany(new Document());
            result += performOneTest(collection) / TESTS_COUNT;
        }
        logger.info("All testss finished for {} threads, {} attempts, everyone inserted {} documents", NUMBER_OF_THREADS, TESTS_COUNT, INSERTS_COUNT);
        logger.info("Average speed: {} millis / {} inserts", result, SPEED_UNIT);
    }

    private static double performOneTest(MongoCollection<Document> collection) {
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        logger.info("Before test collection contains {} documents", collection.count());
        logger.info("Starting to insert {} documents using {} threads", INSERTS_COUNT, NUMBER_OF_THREADS);
        Stopwatch stopwatch = Stopwatch.createStarted();
        final int[] k = {0};
        IntStream.range(0, INSERTS_COUNT)
                .forEach(i -> {
                    executor.submit(() -> {
                        insertDocument(collection);
                        if (incrementSync(k) == INSERTS_COUNT) {
                            logger.info("shutting down");
                            executor.shutdown();
                        }
                    });
                });

        logger.info("waiting for threads to complete");
        while (true) {
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
                if (executor.isTerminated()) {
                    logger.info("completed ALL {} tasks", k[0]);
                    break;
                } else {
                    logger.info("completed {} tasks", k[0]);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        stopwatch.stop(); // optional
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("Insert of {} documents took {} millis", INSERTS_COUNT, millis);
        double speed = millis / ((double) INSERTS_COUNT / SPEED_UNIT);
        logger.info("speed - {} millis/{} inserts", speed, SPEED_UNIT);
        return speed;
    }

    private static void insertDocument(MongoCollection<Document> collection) {
        collection.insertOne(createDocument());
    }

    synchronized static int incrementSync(final int[] k) {
        return ++k[0];
    }

    private static Document createDocument() {
        Document document = new Document()
                .append("UID", UUID.randomUUID().toString())
                .append("description", "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis lacinia vulputate odio. Donec interdum mi vel quam sodales lobortis. In semper volutpat neque, non laoreet quam tincidunt vitae. Duis facilisis dignissim nibh, sed vestibulum nibh euismod ac. Aenean dictum in sem eget viverra. Mauris consequat sollicitudin leo, id euismod nisl vestibulum ut. Aenean sollicitudin libero lectus, nec ultrices mauris porttitor eu. Suspendisse potenti. Vivamus faucibus mollis metus. Curabitur in augue volutpat, ullamcorper risus fringilla, tincidunt odio. Phasellus efficitur suscipit aliquam. Donec congue lacus in leo ultrices, sed pulvinar sapien eleifend. Cras et eros a nunc mollis pharetra a eu ante. Praesent purus ligula, tristique id ipsum et, tempor ultrices tellus. Curabitur a risus et nulla rutrum molestie eu in dui.");
        return document;
    }
}
