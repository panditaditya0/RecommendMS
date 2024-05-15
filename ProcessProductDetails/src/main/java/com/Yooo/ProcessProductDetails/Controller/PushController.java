package com.Yooo.ProcessProductDetails.Controller;

import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class PushController {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);

    @Autowired
    public ImageRepo imageRepo;

    @Autowired
    public ProductDetailService productDetailService;

    @CrossOrigin
    @PostMapping("/push/{brand}")
    public ResponseEntity pushToWeaviate(@PathVariable String brand){
        List<KafkaPayload> allKafkaPayload = imageRepo.findByBrand(brand);
        int chunkCount =   allKafkaPayload.size()/4;
        List<List<KafkaPayload>> chunks = Lists.partition(allKafkaPayload, chunkCount);

        for (List<KafkaPayload> chunk : chunks) {
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


    //As the others mentioned, it's will work. ExecutorService can do the job. Here you can see I used it for starting a video stream that sits on a separate endpoint.

//    @PostMapping("/capture")
//    public ResponseEntity capture() {
//        // some code omitted
//        ExecutorService service=Executors.newCachedThreadPool();
//        service.submit(() ->   gg(a););
//        return return ResponseEntity.ok()
//                .body(stream);
//    }

    public void gg(List<KafkaPayload> allKafkaPayload) {
        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
        LOGGER.info("No of products -> "+ allKafkaPayload.size());
        List<List<KafkaPayload>> inputChunk = Lists.partition(allKafkaPayload, 3);
        for (List<KafkaPayload> aChunk : inputChunk) {
            List<Map<String, Object>> dataObjs = new ArrayList<>();
            for (KafkaPayload kafkaPayload : aChunk) {
                kafkaPayload.base64Image = productDetailService.downloadAndDownSizeImage(baseUrl + kafkaPayload.image_link);
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
