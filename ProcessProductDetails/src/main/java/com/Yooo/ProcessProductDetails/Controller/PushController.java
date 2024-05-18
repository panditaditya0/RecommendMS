package com.Yooo.ProcessProductDetails.Controller;

import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
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
    @PostMapping("/push/{value}")
    public ResponseEntity pushToWeaviate(@PathVariable String value){
        List<KafkaPayload> allKafkaPayload = imageRepo.findByBrand( value);
        int chunkCount =   allKafkaPayload.size()/4;
        List<List<KafkaPayload>> chunks = Lists.partition(allKafkaPayload, chunkCount);

        for (List<KafkaPayload> chunk : chunks) {
//            gg(chunk);
            Runnable thread = new Runnable()
            {
                public void run()
                {
                    gg(chunk);
                }
            };
            System.out.println("Strting from thread: " + thread.toString());
            Thread th=   new Thread(thread);
            th.setDaemon(true);
            th.run();
        }

         return ResponseEntity.ok().build();
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
    public ResponseEntity pushAll(@PathVariable String key) {
        ArrayList<KafkaPayload> allKafkaPayload = imageRepo.listOfAllProducts();
        int chunkCount = allKafkaPayload.size() / 5;
        List<List<KafkaPayload>> chunks = Lists.partition(allKafkaPayload, chunkCount);

        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (List<KafkaPayload> sublist : chunks) {
            executor.submit(() -> gg(sublist));
        }
        executor.shutdown();

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @GetMapping("/push/allImages")
    public ResponseEntity pushAllImages(){
        List<String> allSkuIds = imageRepo.listOfAllSkuIds();
        List<List<String>> chunks = Lists.partition(allSkuIds, 4);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (List<String> sublist : chunks) {
            executor.submit(() ->{
                    ArrayList<KafkaPayload> listOfKafkaProducts = imageRepo.getListOfProducts(sublist);
                    gg(listOfKafkaProducts);
                    chunks.remove(listOfKafkaProducts);
                    System.gc();

            });
        }
        executor.shutdown();


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

    public void gg(List<KafkaPayload> allKafkaPayload) {
        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
        LOGGER.info("No of products -> "+ allKafkaPayload.size());
        List<List<KafkaPayload>> inputChunk = Lists.partition(allKafkaPayload, 3);
        allKafkaPayload = null;
        System.gc();
        for (List<KafkaPayload> aChunk : inputChunk) {
            List<Map<String, Object>> dataObjs = new ArrayList<>();
            for (KafkaPayload kafkaPayload : aChunk) {
                if(null == kafkaPayload.base64Image || kafkaPayload.base64Image.length() < 10){
                    kafkaPayload.base64Image = productDetailService.downloadAndDownSizeImage(baseUrl + kafkaPayload.image_link);
                }
                Optional productDetailsOptional = imageRepo.findById(kafkaPayload.getEntity_id());

                if(!productDetailsOptional.isEmpty()) {
                    KafkaPayload finalObject1 = productDetailService.updateProductDetailsToDb(kafkaPayload, productDetailsOptional);

                    List<String> childCategories = new ArrayList<>();
                    Iterator it = finalObject1.child_categories.iterator();
                    while (it.hasNext()) {
                        ChildCategoryModel a = (ChildCategoryModel) it.next();
                        childCategories.add(a.getLabel());
                    }

                    Map<String, Object> properties = new HashMap<>();
                    properties.put("image", finalObject1.base64Image);
                    properties.put("entity_id", String.valueOf(finalObject1.entity_id));
                    properties.put("sku_id", finalObject1.sku_id);
                    properties.put("product_id", finalObject1.product_id);
                    properties.put("title", finalObject1.title);
                    properties.put("discounted_price", finalObject1.discount);
                    properties.put("region_sale_price", finalObject1.price_in);
                    properties.put("brand", finalObject1.brand);
                    properties.put("image_link", baseUrl + finalObject1.image_link);
                    properties.put("link", finalObject1.link);
                    properties.put("mad_id", "");
                    properties.put("sale_price", finalObject1.special_price_in);
                    properties.put("price", finalObject1.price_in);
                    properties.put("uuid", finalObject1.uuid.toString());
                    properties.put("parentCategory", finalObject1.parent_category);
                    properties.put("childCategories", childCategories.toArray());
                    properties.put("color", finalObject1.color);
                    dataObjs.add(properties);
                } else {
                    LOGGER.info("No product details found for id " + kafkaPayload.entity_id);
                }
            }
            productDetailService.pushToVectorDb(dataObjs);
        }
    }
}

