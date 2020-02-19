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
import java.util.*;
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

                    int total = response.asJson().path("hits").path("total").path("value").asInt(1);
                    int totalPage = (int) Math.ceil(total / size);
                    if (totalPage == 0) {
                        totalPage = 1;
                    }
                    JsonNode jsonNode = response.asJson().path("hits").path("hits");
                    if (!jsonNode.isMissingNode()) {
                        Iterator<JsonNode> it = jsonNode.iterator();
                        while (it.hasNext()) {
                            searchedHeroes.add(SearchedHero.fromJson(it.next().get("_source")));
                        }
                    }
                    return new PaginatedResults<>(total, page, totalPage, searchedHeroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{" +
                        "    \"suggest\": {\n" +
                        "        \"heroes-suggest\" : {\n" +
                        "            \"prefix\" : \"" + input + "\", \n" +
                        "            \"completion\" : { \n" +
                        "                \"field\" : \"suggest\" \n" +
                        "            }\n" +
                        "        }\n" +
                        "    }" +
                        "}"))
                .thenApply(response -> {
                    List<SearchedHero> searchedHeroes = new ArrayList<>();
                    JsonNode globalJsonNode = response.asJson().path("suggest").path("heroes-suggest").path(0).path("options");
                    if (!globalJsonNode.isMissingNode()) {
                        Iterator<JsonNode> it = globalJsonNode.iterator();
                        while (it.hasNext()) {
                            JsonNode jsonNode = it.next();
                            SearchedHero searchedHero = SearchedHero.fromJson(jsonNode.get("_source"));
                            searchedHeroes.add(searchedHero);
                        }
                    }
                    return searchedHeroes;
                });

    }
}
