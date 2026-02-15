package net.naour.kafkaspringcloudstream.handlers;

import net.naour.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) -> {
            System.out.println("**************************");
            System.out.println(input.toString());
            System.out.println("**************************");

        };
    }

    //chque seconde envoyer un message sur le topic kafka
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> {
            return new PageEvent(
                    Math.random() > 0.5 ? "P1" : "P2",
                    Math.random() > 0.5 ? "U1" : "U2",
                    new Date(),
                    10+ new Random().nextInt(10000)
                    );

        };
    }

    @Bean       //input key,value                       //output key value(nombre de visite)
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input ->
                input
                        .filter((key, value) ->
                                value.duration() > 100)
                        .map((key, value) ->
                                new KeyValue<>(value.name(), value.duration()))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))) // adapte: 5s ici
                        .count(Materialized.as("count-store"))//sur une page ou la table sappelle count store
                        .toStream()
                        .map((windowedKey, count) -> new KeyValue<>(windowedKey.key(), count));
    }

}
