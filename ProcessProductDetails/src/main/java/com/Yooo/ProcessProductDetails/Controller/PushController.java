package com.Yooo.ProcessProductDetails.Controller;

import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class PushController {

    @Autowired
    public ImageRepo imageRepo;

    @Autowired
    public ProductDetailService productDetailService;

    @CrossOrigin
    @PostMapping("/push")
    public void pushToWeaviate(){
        Runnable thread = new Runnable()
        {
            public void run()
            {
                gg();
            }
        };
        System.out.println("Starting thread..");
        new Thread(thread).run();

    }

    public void gg() {
        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
        List<KafkaPayload> allKafkaPayload = imageRepo.findByBrand();
        List<List<KafkaPayload>> inputChunk = Lists.partition(allKafkaPayload, 3);
        for (List<KafkaPayload> aChunk : inputChunk) {
            List<Map<String, Object>> dataObjs = new ArrayList<>();
            for (KafkaPayload kafkaPayload : aChunk) {
                kafkaPayload.base64Image = productDetailService.downloadAndDownSizeImage(baseUrl + kafkaPayload.image_link);
                KafkaPayload finalObject1 = productDetailService.updateProductDetailsToDb(kafkaPayload);

                List<String> childCategories = new ArrayList<>();
                Iterator it = finalObject1.childCategories.iterator();
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
                properties.put("discounted_price", finalObject1.discounted_price);
                properties.put("region_sale_price", finalObject1.region_sale_price);
                properties.put("brand", finalObject1.brand);
                properties.put("image_link", baseUrl + finalObject1.image_link);
                properties.put("link", finalObject1.link);
                properties.put("mad_id", finalObject1.mad_id);
                properties.put("sale_price", finalObject1.sale_price);
                properties.put("price", finalObject1.price);
                properties.put("uuid", finalObject1.uuid.toString());
                properties.put("parentCategory", finalObject1.parentCategory);
                properties.put("childCategories", childCategories.toArray());
                properties.put("color", finalObject1.color);
                dataObjs.add(properties);
            }
            productDetailService.pushToVectorDb(dataObjs);
        }
    }
}
