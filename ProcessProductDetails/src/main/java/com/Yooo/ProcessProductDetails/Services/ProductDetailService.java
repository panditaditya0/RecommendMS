package com.Yooo.ProcessProductDetails.Services;

import com.Yooo.ProcessProductDetails.Model.CategoryModel;
import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.misc.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URL;
import java.util.*;

@Service
public class ProductDetailService {
    private Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);
    public ImageRepo imageRepo;
    public final String className = "TestImg12";  // Replace with your class name
    public static Set<String> PARENT_CATEGORY = new HashSet<>(Arrays.asList("lehenga", "sarees", "gown", "dresses", "kurta set", "jacket", "jumpsuits", "sharara set", "co-ord set","designer ghararas", "resort and beach wear","bralettes for women", "kurtas for women","designer anarkali", "jackets for women","pants","kaftans","tops","skirts"));

    public ProductDetailService(ImageRepo imageRep){
        this.imageRepo = imageRep;
    }
    private KafkaPayload updateProductDetailsToDb(KafkaPayload productDetails) {
        try{
            Optional op = imageRepo.findById(productDetails.getEntity_id());
            if(op.isPresent()){
                KafkaPayload productDetailsFromSb = (KafkaPayload) op.get();
                productDetails.setUuid(productDetailsFromSb.getUuid());
            } else {
                LOGGER.error("ERROR UPDATING "+ productDetails.getEntity_id());
            }
        } catch (Exception ex){
            LOGGER.error("ERROR UPDATING "+ productDetails.getEntity_id());
        }
        LOGGER.debug("UPDATED IN DB "+ productDetails.getEntity_id());
        return imageRepo.save(productDetails);
    }

    private void saveImageDetailsToDb(KafkaPayload productDetails ) {
        try{
            productDetails.uuid = UUID.randomUUID().toString();
            imageRepo.save(productDetails);
        } catch (Exception ex){
            LOGGER.error("ERROR FOR "+ productDetails.getEntity_id()+ ex.getMessage() + ex.getStackTrace());
        }
    }

    public void processProductDetails(List<RequestPayload> allProductDetails) {
        Config config = new Config("http", "164.92.160.25:8080");
        WeaviateClient client = new WeaviateClient(config);
        Result<Meta> meta = client.misc().metaGetter().run();
        if (meta.getError() == null) {
            System.out.printf("meta.hostname: %s\n", meta.getResult().getHostname());
            System.out.printf("meta.version: %s\n", meta.getResult().getVersion());
            System.out.printf("meta.modules: %s\n", meta.getResult().getModules());
        } else {
            System.out.printf("Error: %s\n", meta.getError().getMessages());
        }

        List<Map<String, Object>> dataObjs = new ArrayList<>();
        for (RequestPayload productDetails : allProductDetails) {
            KafkaPayload aKafkaProductPayload = this.parentChildCategoryCorrection(productDetails);
            Optional productDetailsOptional = imageRepo.findById(aKafkaProductPayload.getEntity_id());
            KafkaPayload finalObject = aKafkaProductPayload;
            if (productDetailsOptional.isEmpty()) {
                this.saveImageDetailsToDb(aKafkaProductPayload);
            } else {
                finalObject = this.updateProductDetailsToDb(aKafkaProductPayload);
            }

            KafkaPayload finalObject1 = aKafkaProductPayload;
            if(Boolean.valueOf(System.getenv("download_image")) && finalObject1.brand.equalsIgnoreCase("Kalighata")){
//            if(true) {
                String baseUrl = System.getenv("download_image_base_url");
//                String baseUrl = "https://dimension-six.perniaspopupshop.com/media/catalog/product";

                try {
                    URL url = new URL(baseUrl+finalObject1.image_link);
                    InputStream inputStream = url.openStream();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                    byte[] imageBytes = outputStream.toByteArray();
                    String base64Image = Base64.getEncoder().encodeToString(imageBytes);
                    base64Image.replace("\n", "").replace(" ", "");
                    inputStream.close();
                    outputStream.close();

                    List<String> childCategories = new ArrayList<>();
                    Iterator it = finalObject1.childCategories.iterator();
                    while(it.hasNext()){
                        ChildCategoryModel a =(ChildCategoryModel) it.next();
                        childCategories.add(a.getLabel());
                    }

                    Map<String, Object> properties = new HashMap<>();
                    properties.put("image", base64Image);
                    properties.put("entity_id", String.valueOf(finalObject1.entity_id));
                    properties.put("sku_id", finalObject1.sku_id);
                    properties.put("product_id", finalObject1.product_id);
                    properties.put("title", finalObject1.title);
                    properties.put("discounted_price",finalObject1.discounted_price);
                    properties.put("region_sale_price", finalObject1.region_sale_price);
                    properties.put("brand", finalObject1.brand);
                    properties.put("image_link", baseUrl+finalObject1.image_link);
                    properties.put("link", finalObject1.link);
                    properties.put("mad_id", finalObject1.mad_id);
                    properties.put("sale_price", finalObject1.sale_price);
                    properties.put("price", finalObject1.price);
                    properties.put("uuid", finalObject1.uuid.toString());
                    properties.put("parentCategory", finalObject1.parentCategory);
                    properties.put("childCategories",childCategories.toArray());
                    dataObjs.add(properties);
                    LOGGER.info("COMPLETED -> " + finalObject1.sku_id + " ");
                } catch (Exception ex) {
                    LOGGER.error("ERROR -> " + finalObject1.sku_id + " " + ex.getMessage() + ex.getStackTrace());
                }
            }
        }

        List<List<Map<String, Object>>> chunk = Lists.partition(dataObjs, 10);
        ObjectsBatcher batcher = client.batch().objectsBatcher();
        for (List<Map<String, Object>> properties2 : chunk) {
            for (Map<String, Object> prop :properties2)
            batcher.withObject(WeaviateObject.builder()
                    .className(className)
                    .properties(prop)
                    .id(prop.get("uuid").toString())
                    .build()
            );
        }
        Result<ObjectGetResponse[]> a =  batcher.run();

        for(ObjectGetResponse b : a.getResult()){
            if(!(b.getResult().toString().contains("SUCCESS"))){
                LOGGER.error("ERROR while bulk import -> " + b.getId());
            }
        }
    }

    private KafkaPayload parentChildCategoryCorrection(RequestPayload productDetails) {
        Set<ChildCategoryModel> allChildLabel =new HashSet<>() ;
        String parentLabel = "NoParentFound";
        for (CategoryModel aCategoryModel : productDetails.getCategory()){
            String tempLabel=aCategoryModel.label.toLowerCase();
            if(!PARENT_CATEGORY.contains(tempLabel)){
                ChildCategoryModel childCategoryModel = new ChildCategoryModel();
                childCategoryModel.setLabel(tempLabel);
                allChildLabel.add(childCategoryModel);
            } else{
                parentLabel = tempLabel;
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        KafkaPayload aPayload = mapper.convertValue(productDetails, KafkaPayload.class);
        aPayload.setParentCategory(parentLabel);
        aPayload.setChildCategories(allChildLabel);
        return aPayload;
    }
}
