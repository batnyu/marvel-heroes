package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
//        return CompletableFuture.completedFuture(new PaginatedResults<>(3, 1, 1, Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan())));
        // TODO
        int from = ((page - 1) * size);
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{" +
                        "  \"from\": " + from + "," +
                        "  \"size\": " + size + "," +
                        "  \"query\": {\n" +
                        "    \"multi_match\" : {\n" +
                        "      \"query\" : \"" + input + "\",\n" +
                        "      \"fields\" : [ \"name^4\", \"aliases^3\", \"secretIdentities^3\", \"description^2\", \"partners^1\"] \n" +
                        "    }\n" +
                        "  }" +
                        "}"))
                .thenApply(response -> {
                    List<SearchedHero> searchedHeroes = new ArrayList<>();
                    int total = response.asJson().get("hits").get("total").get("value").asInt();
                    Iterator<JsonNode> it = response.asJson().get("hits").get("hits").iterator();
                    while (it.hasNext()) {
                        searchedHeroes.add(SearchedHero.fromJson(it.next().get("_source")));
                    }
                    int totalPage = (int) Math.ceil(total / size);
                    if (totalPage == 0) {
                        totalPage = 1;
                    }
                    System.out.println("total " + total);
                    System.out.println("size " + size);
                    System.out.println("totalPage " + totalPage);
                    System.out.println("page " + page);
                    return new PaginatedResults<>(total, page, totalPage, searchedHeroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        // TODO
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{" +
                        "    \"suggest\": {\n" +
                        "        \"heroes-suggest\" : {\n" +
                        "            \"prefix\" : \"" + input + "\", \n" +
                        "            \"completion\" : { \n" +
                        "                \"field\" : \"name_suggest\" \n" +
                        "            }\n" +
                        "        }\n" +
                        "    }" +
                        "}"))
                .thenApply(response -> {
                    System.out.println(response.asJson());
                    List<SearchedHero> searchedHeroes = new ArrayList<>();
                    Iterator<JsonNode> it = response.asJson().get("suggest").get("heroes-suggest").get(0).get("options").iterator();
                    while (it.hasNext()) {
                        JsonNode jsonNode = it.next();
                        SearchedHero searchedHero = SearchedHero.fromJson(jsonNode.get("_source"));
                        searchedHeroes.add(searchedHero);
                    }
                    return searchedHeroes;
                });

    }
}
