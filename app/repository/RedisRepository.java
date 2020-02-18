package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");

    private final String nameSet = "top";
    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        RedisFuture<Double> future = commands.zincrby(nameSet, 1, statItem.toJson().toString());

        return future.thenApply(response -> {
            return true;
        });
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        RedisFuture<Long> future = commands.zadd(nameSet, ZAddArgs.Builder.nx(), 0, statItem.toJson().toString());

        return future.thenApply(response -> {
            return response;
        });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        RedisFuture<List<String>> future = commands.zrange(nameSet, 0, count - 1);

        return future.thenApply(responses -> {
            List<StatItem> statItemList = new ArrayList<>();
            for (String response : responses) {
                StatItem statItem = StatItem.fromJson(response);
                statItemList.add(statItem);
            }

            return statItemList;
        });
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        RedisFuture<List<ScoredValue<String>>> future = commands.zrevrangeWithScores(nameSet, 0, count - 1);

        return future.thenApply(responses -> {
            List<TopStatItem> topStatItemList = new ArrayList<>();
            for (ScoredValue<String> response : responses) {
                StatItem statItem = StatItem.fromJson(response.getValue());
                TopStatItem topStatItem = new TopStatItem(statItem, (long) response.getScore());
                topStatItemList.add(topStatItem);
            }

            return topStatItemList;
        });
    }
}
