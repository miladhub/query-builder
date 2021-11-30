package querytest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.vavr.collection.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.*;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.contains;

public class MongoTest
{
    @Test
    public void test_find_all() {
        try (MongoClient mongoClient = MongoClients.create()) {
            MongoDatabase test = mongoClient.getDatabase("test");
            for (Document doc : test.getCollection("foo").find()) {
                System.out.println(doc.toJson());
            }
        }
    }

    @Test
    public void test_sum() {
        try (MongoClient mongoClient = MongoClients.create()) {
            MongoDatabase test = mongoClient.getDatabase("test");
            for (Document doc : test.getCollection("foo").aggregate(
                    Arrays.asList(
                            group("$foo_str", sum("sum_foo_int", "$foo_int")),
                            project(fields(
                                    include("sum_foo_int"),
                                    new BsonDocument("foo_str", new BsonString("$_id")),
                                    excludeId()))))
            ) {
                System.out.println(doc.toJson());
            }
        }
    }

    @Test
    public void test_avg() {
        try (MongoClient mongoClient = MongoClients.create()) {
            MongoDatabase test = mongoClient.getDatabase("test");
            for (Document doc : test.getCollection("foo").aggregate(
                    Arrays.asList(
                            group("$foo_str", avg("avg_foo_int", "$foo_int")),
                            project(fields(
                                    include("avg_foo_int"),
                                    new BsonDocument("foo_str", new BsonString("$_id")),
                                    excludeId()))))
            ) {
                System.out.println(doc.toJson());
            }
        }
    }

    @Test
    public void test_or_matcher() {
        assertThat(50, anyOf(
                is(50),
                is(50.0)));

        assertThat(50.0, anyOf(
                is(50),
                is(50.0)));

        assertThat(42, anyOf(
                is(42),
                is(42.0)));

        assertThat(42.0, anyOf(
                is(42),
                is(42.0)));

        assertThat(42, comparesEqualTo(42));

        assertThat(List.of("foo", "bar"), contains(is("foo"), anyOf(is("bar"),
                is("baz"))));

        assertThat(List.of("foo", "baz"), contains(is("foo"), anyOf(is("bar"),
                is("baz"))));
    }

    @Test
    public void test_count() {
        try (MongoClient mongoClient = MongoClients.create()) {
            MongoDatabase test = mongoClient.getDatabase("test");
            for (Document doc : test.getCollection("foo").aggregate(
                    Arrays.asList(
                            group("$foo_str", sum("count_foo_int", 1)),
                            project(fields(
                                    include("count_foo_int"),
                                    new BsonDocument("foo_str", new BsonString("$_id")),
                                    excludeId()))))
            ) {
                System.out.println(doc.toJson());
            }
        }
    }
}
