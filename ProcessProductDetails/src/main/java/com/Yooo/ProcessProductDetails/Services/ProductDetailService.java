package com.Yooo.ProcessProductDetails.Services;

import com.Yooo.ProcessProductDetails.Model.CategoryModel;
import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
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
    public final String className = "TestImg6";  // Replace with your class name
    public static Set<String> PARENT_CATEGORY = new HashSet<>(Arrays.asList("lehengas", "sarees"));

    public ProductDetailService(ImageRepo imageRep){
        this.imageRepo = imageRep;
    }
    private KafkaPayload updateProductDetailsToDb(KafkaPayload productDetails) {
        Optional op = imageRepo.findById(productDetails.getEntity_id());
        if(op.isPresent()){
            KafkaPayload productDetailsFromSb = (KafkaPayload) op.get();
            productDetails.setUuid(productDetailsFromSb.getUuid());
            return imageRepo.save(productDetails);
        } else {
            LOGGER.error("ERROR UPDATING "+ productDetails.getEntity_id());
        }
        return null;
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
        for (RequestPayload productDetails : allProductDetails) {
            KafkaPayload aKafkaProductPayload = this.parentChildCategoryCorrection(productDetails);
            Config config = new Config("http", "164.92.160.25:9090");
            WeaviateClient client = new WeaviateClient(config);
            Result<Meta> meta = client.misc().metaGetter().run();
            if (meta.getError() == null) {
                System.out.printf("meta.hostname: %s\n", meta.getResult().getHostname());
                System.out.printf("meta.version: %s\n", meta.getResult().getVersion());
                System.out.printf("meta.modules: %s\n", meta.getResult().getModules());
            } else {
                System.out.printf("Error: %s\n", meta.getError().getMessages());
            }
            Optional productDetailsOptional = imageRepo.findById(aKafkaProductPayload.getEntity_id());
            KafkaPayload finalObject = aKafkaProductPayload;
            if (productDetailsOptional.isEmpty()) {
                LOGGER.info("Entity if -> " + aKafkaProductPayload.getEntity_id() + " not present, creating new entry in db");
                this.saveImageDetailsToDb(aKafkaProductPayload);
            } else {
                LOGGER.info("Entity if -> " + aKafkaProductPayload.getEntity_id() + " exists, updating entry in db");
                finalObject = this.updateProductDetailsToDb(aKafkaProductPayload);
            }
            KafkaPayload finalObject1 = aKafkaProductPayload;
            if(Boolean.valueOf(System.getenv("download_image"))){
                String baseUrl = System.getenv("download_image_base_url");
                try {
                    URL url = new URL(finalObject1.image_link);
                    InputStream inputStream = url.openStream();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                    byte[] imageBytes = outputStream.toByteArray();
                    String base64Image = Base64.getEncoder().encodeToString(imageBytes);
                    inputStream.close();
                    outputStream.close();

                    Result<WeaviateObject> result = client.data().creator()
                            .withClassName(className)
                            .withProperties(new HashMap<String, Object>() {{
                                put("image", base64Image);
                                put("entity_id", String.valueOf(finalObject1.entity_id));
                                put("sku_id", finalObject1.sku_id);
                                put("product_id", finalObject1.product_id);
                                put("title", finalObject1.title);
                                put("discounted_price", String.valueOf(finalObject1.discounted_price));
                                put("region_sale_price", String.valueOf(finalObject1.region_sale_price));
                                put("brand", finalObject1.brand);
                                put("image_link", baseUrl+finalObject1.image_link);
//                        put("discount", finalObject1.discount);
                                put("link", finalObject1.link);
                                put("mad_id", finalObject1.mad_id);
                                put("sale_price", String.valueOf(finalObject1.sale_price));
                                put("price", String.valueOf(finalObject1.price));
                                put("uuid", finalObject1.uuid.toString());
                            }})
                            .withID(finalObject1.uuid.toString())
                            .withVector(Collections.nCopies(1536, 0.12345f).toArray(new Float[0]))
                            .run();

                    LOGGER.info("RESPONSE -> " + finalObject1.sku_id + " " + result.toString());
                } catch (Exception ex) {
                    LOGGER.error("ERROR -> " + finalObject1.sku_id + " " + ex.getMessage() + ex.getStackTrace());
                }
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
