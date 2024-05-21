package com.Yooo.ProcessProductDetails.Controller;

import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class PushController {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);

    @Autowired
    public ImageRepo imageRepo;

    @Autowired
    public ProductDetailService productDetailService;

    @Autowired
    private RedisTemplate template;

    @CrossOrigin
    @GetMapping("/push/{value}")
    public ResponseEntity pushToWeaviate(@PathVariable String value) {
        ArrayList<String> allSkuIds = imageRepo.findByParent(value);
        List<List<String>> chunks = Lists.partition(allSkuIds, 3);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        AtomicInteger counter = new AtomicInteger();
        executor.submit(() -> {
            for (List<String> sublist : chunks) {
                ArrayList<KafkaPayload> listOfKafkaProducts = imageRepo.getListOfProducts(sublist);
                productDetailService.gg(listOfKafkaProducts);
                counter.addAndGet(3);
                LOGGER.info( value + "-> processed " + counter.get() + " product details");
            }
            LOGGER.info( value + " -> Compoleted");
        });

        return ResponseEntity.ok("Products -> " + allSkuIds.size());
    }

    @CrossOrigin
    @GetMapping("/crawler/{key}/{value}")
    public ResponseEntity crawler(@PathVariable String key, @PathVariable String value){
        ArrayList<String> allKafkaPayload = imageRepo.listOfSkuId(value);

        List<List<String>> sublists = divideList(allKafkaPayload, 30);
        ExecutorService executor = Executors.newFixedThreadPool(30);
        for (List<String> sublist : sublists) {
            executor.submit(() -> makeApiCalls(sublist));
        }
        executor.shutdown();

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @GetMapping("/pushAll")
    public ResponseEntity pushAll() {
        List<String> allSkuIds = imageRepo.listOfAllSkuIds();
        List<List<String>> chunks = Lists.partition(allSkuIds, 3);
        for (List<String> sublist : chunks) {
            ArrayList<KafkaPayload> listOfKafkaProducts = imageRepo.getListOfProducts(sublist);
            productDetailService.gg(listOfKafkaProducts);
        }
        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @GetMapping("/push/allImages")
    public ResponseEntity pushAllImages(){
        List<String> allSkuIds = imageRepo.listOfAllSkuIds();
        List<List<String>> chunks = Lists.partition(allSkuIds, 3);
        for (List<String> sublist : chunks) {
            ArrayList<KafkaPayload> listOfKafkaProducts = imageRepo.getListOfProducts(sublist);
            productDetailService.gg(listOfKafkaProducts);
        }
        return ResponseEntity.ok().build();
    }

    private static List<List<String>> divideList(ArrayList<String> list, int numParts) {
        List<List<String>> sublists = new ArrayList<>();
        int chunkSize = (int) Math.ceil((double) list.size() / numParts);

        for (int i = 0; i < list.size(); i += chunkSize) {
            sublists.add(new ArrayList<>(list.subList(i, Math.min(list.size(), i + chunkSize))));
        }

        return sublists;
    }
    private LinkedHashSet<String> getSkus(String key){
        Object listOfSkus = template.opsForList().range(key+"YES", 0, -1);
        LinkedHashSet<String> listOfSkuIdsFromRedis = new LinkedHashSet<>();
        listOfSkuIdsFromRedis.addAll((ArrayList<String>) listOfSkus);
        return listOfSkuIdsFromRedis;
    }

    private void makeApiCalls(final List<String> skuIds) {
        HttpClient client = HttpClient.newHttpClient();
        for (String skuId : skuIds) {
            try {
                if(getSkus(skuId).size() > 0){
                    continue;
                }

                    long startTime = System.nanoTime();
                HttpRequest request = HttpRequest.newBuilder()
//                        .uri(new URI("http://164.92.160.25:7074/fetch"))
                        .uri(new URI("http://localhost:8080/fetch"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"num_results\":[15],\"sku_id\":\""+skuId+"\" ,\"widget_list\":[0],\"mad_uuid\":\"i_am_a_bot\",\"user_id\":\"\",\"product_id\":\"seema-gujral-ivory-sequins-embroidered-lehenga-set-segc22102\",\"details\":true,\"fields\":[\"brand\",\"sku_id\",\"region_sale_price\",\"discount_label\",\"discount\"],\"extra_params\":{\"exchange_rate\":1},\"region\":\"ind\",\"filters\":[{\"field\":\"brand\",\"value\":[\"Seema \"]}]}"))
                        .build();

                HttpRequest request2 = HttpRequest.newBuilder()
                        .uri(new URI("http://164.92.160.25:7074/fetch"))
//                        .uri(new URI("http://localhost:8080/fetch"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"num_results\":[15],\"sku_id\":\""+skuId+"\" ,\"widget_list\":[0],\"mad_uuid\":\"i_am_a_bot\",\"user_id\":\"\",\"product_id\":\"seema-gujral-ivory-sequins-embroidered-lehenga-set-segc22102\",\"details\":true,\"fields\":[\"brand\",\"sku_id\",\"region_sale_price\",\"discount_label\",\"discount\"],\"extra_params\":{\"exchange_rate\":1},\"region\":\"ind\",\"filters\":[{\"field\":\"brand\",\"value\":[\"Seema \"]}]}"))
                        .build();


                HttpRequest request3 = HttpRequest.newBuilder()
                        .uri(new URI("http://164.92.160.25:7074/fetch"))
//                        .uri(new URI("http://localhost:8080/fetch"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"num_results\":[15],\"sku_id\":\""+skuId+"\" ,\"widget_list\":[0],\"mad_uuid\":\"i_am_a_bot\",\"user_id\":\"\",\"product_id\":\"seema-gujral-ivory-sequins-embroidered-lehenga-set-segc22102\",\"details\":true,\"fields\":[\"brand\",\"sku_id\",\"region_sale_price\",\"discount_label\",\"discount\"],\"extra_params\":{\"exchange_rate\":1},\"region\":\"ind\",\"filters\":[{\"field\":\"brand\",\"value\":[\"Seema \"]}]}"))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long endTime = System.nanoTime();
                long executionTime = endTime - startTime;
                if (response.statusCode() == 200 && response.body().contains("sku_id")) {
                    System.out.println("OK -> " + executionTime/1000000000 + "sec");
                }
//                System.out.println("STATUS: " + response.statusCode());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Completed");
    }
}

