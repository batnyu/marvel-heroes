package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.HeroSamples;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
         String query = "{ id : \"" +  heroId + "\"}";
         Document document = Document.parse(query);
         return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        List<Document> pipeline = Arrays.asList();
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents ->
                        documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList())
                );
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        List<Document> pipeline = Arrays.asList(
                Document.parse("{ $unwind: \"$powers\"}"),
                Document.parse("{ $group: { _id: \"$powers\", count: { $sum: 1 } } }"),
                Document.parse("{ $sort: { count: -1 } }"),
                Document.parse("{ $limit: " + top + " }")
        );
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents ->
                     documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                             .collect(Collectors.toList())
                 );
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        List<Document> pipeline = Arrays.asList(
                Document.parse("{ $group: { _id: \"$identity.universe\", count: { $sum: 1 } } }")
        );
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
             .thenApply(documents ->
                 documents.stream()
                     .map(Document::toJson)
                     .map(Json::parse)
                     .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                     .collect(Collectors.toList())
             );
    }

}
