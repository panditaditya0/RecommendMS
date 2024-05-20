package com.Yooo.ProcessProductDetails.Services;

import com.Yooo.ProcessProductDetails.Config.SingleWeaviateClient;
import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;

@Service
public class ProductDetailService {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);
    public ImageRepo imageRepo;
    public final String className = "TestImg16";  // Replace with your class name
    private SingleWeaviateClient singleWeaviateClient;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    public ProductDetailService(ImageRepo imageRep, SingleWeaviateClient singleWeaviateClient) {
        this.imageRepo = imageRep;
        this.singleWeaviateClient = singleWeaviateClient;
    }

    public KafkaPayload updateProductDetailsToDb(KafkaPayload newData, Optional oldData) {
        try {
            KafkaPayload productDetailsFromSb = (KafkaPayload) oldData.get();
            newData.setUuid(productDetailsFromSb.getUuid());
        } catch (Exception ex) {
           throw new RuntimeException("Error updating product details"+newData.entity_id+" " + ex.getMessage());
        }
        return imageRepo.save(newData);
    }

    private KafkaPayload saveImageDetailsToDb(KafkaPayload productDetails) {
        try {
            productDetails.setUuid(UUID.randomUUID().toString());
            imageRepo.save(productDetails);
            LOGGER.info("NEW ENTRY " + productDetails.getEntity_id());
        } catch (Exception ex) {
            LOGGER.error("ERROR FOR " + productDetails.getEntity_id() + ex.getMessage() + ex.getStackTrace());
        }
        return productDetails;
    }

    public void processProductDetails(List<HashMap<String, Object>> allProductDetails) {
        ArrayList<KafkaPayload> allProductsDetails = new ArrayList<>();
        try {
            for (HashMap<String, Object> map : allProductDetails) {
                KafkaPayload fromDbOptional= imageRepo.findById(Long.parseLong((String) map.get("entity_id"))).get();

                ArrayList<String> child = (ArrayList<String>) map.get("child_categories");
                Set<ChildCategoryModel> child_categories = new HashSet<>();
                KafkaPayload payload = new KafkaPayload();
                for (String category : child) {
                    ChildCategoryModel aChildModel = new ChildCategoryModel();
                    aChildModel.setKafka_entity_id(Long.parseLong((String) map.get("entity_id")));
                    aChildModel.setLabel(category);
                    child_categories.add(aChildModel);
                }

                for(ChildCategoryModel a :child_categories){
                    if(a.kafka_entity_id==0 || a.kafka_entity_id <2){
                        System.out.println("why");
                    }else{
                        System.out.println("OK -> "+ a.kafka_entity_id);
                    }
                }
                payload.setEntity_id(Long.parseLong((String) map.get("entity_id")));
                payload.setSku_id((String) map.get("sku_id"));
                payload.setProduct_id((String) map.get("product_id"));
                payload.setTitle((String) map.get("title"));
                payload.setBrand((String) map.get("brand"));
                payload.setImage_link((String) map.get("image_link"));
                payload.setDiscount((double) map.get("discount"));
                payload.setLink((String) map.get("link"));
                payload.setColor((String) map.get("color"));
                payload.setDomain((String) map.get("domain"));
                payload.setParent_category((String) map.get("parent_category"));
                payload.setPrice_in((double) map.get("price_in"));
                payload.setDiscount_in((double) map.get("discount_in"));
                payload.setSpecial_price_in((double) map.get("special_price_in"));
                payload.setPrice_us((double) map.get("price_us"));
                payload.setDiscount_us((double) map.get("discount_us"));
                payload.setSpecial_price_us((double) map.get("special_price_us"));
                payload.setPrice_row((double) map.get("price_row"));
                payload.setDiscount_row((double) map.get("discount_row"));
                payload.setSpecial_price_row((double) map.get("special_price_row"));
                payload.setUpdated_at(LocalDateTime.parse(LocalDateTime.now().format(dateTimeFormatter), dateTimeFormatter));
                payload.setChild_categories(child_categories);
                payload.setBase64Image(fromDbOptional.base64Image);
                allProductsDetails.add(payload);

                Optional productDetailsOptional = imageRepo.findById(payload.getEntity_id());
                KafkaPayload finalObject = new KafkaPayload();
//                aKafkaProductPayload.base64Image = downloadAndDownSizeImagedownloadAndDownSizeImage(baseUrl+ productDetails.image_link);
                if (productDetailsOptional.isEmpty()) {
                    finalObject = this.saveImageDetailsToDb(payload);
                } else {
                    finalObject = this.updateProductDetailsToDb(payload, productDetailsOptional);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("ERROR Converting " + ex.getMessage());
        }

//        List<Map<String, Object>> dataObjs = new ArrayList<>();
//        List<RequestPayload> newAllProductDetails = allProductDetails.stream()
//                .filter(product -> product != null && product.getSku_id() != null)
//                .collect(Collectors.collectingAndThen(
//                        Collectors.toMap(RequestPayload::getSku_id, p -> p, (p1, p2) -> p1), // Use sku as key for distinct
//                        m -> new ArrayList<>(m.values()))); // Convert map values to list
//
//                        String baseUrl = System.getenv("download_image_base_url");
////                            String baseUrl = "https://dimension-six.perniaspopupshop.com/media/catalog/product";
////        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
//        try {
//                for (KafkaPayload productDetails : chunk) {
//                    KafkaPayload aKafkaProductPayload = this.parentChildCategoryCorrection(productDetails);
//                    Optional productDetailsOptional = imageRepo.findById(aKafkaProductPayload.getEntity_id());
//                    KafkaPayload finalObject = aKafkaProductPayload;
//                    aKafkaProductPayload.base64Image = downloadAndDownSizeImage(baseUrl+ productDetails.image_link);
//                    if (productDetailsOptional.isEmpty()) {
//                        finalObject = this.saveImageDetailsToDb(aKafkaProductPayload);
//                    } else {
//                        finalObject = this.updateProductDetailsToDb(aKafkaProductPayload);
//                    }
//
//                    KafkaPayload finalObject1 = finalObject;
//                    if (true) {
//                        if (true) {
//                            try {
//                                List<String> childCategories = new ArrayList<>();
//                                Iterator it = finalObject1.childCategories.iterator();
//                                while (it.hasNext()) {
//                                    ChildCategoryModel a = (ChildCategoryModel) it.next();
//                                    childCategories.add(a.getLabel());
//                                }
//
//                                Map<String, Object> properties = new HashMap<>();
//                                properties.put("image", finalObject1.base64Image);
//                                properties.put("entity_id", String.valueOf(finalObject1.entity_id));
//                                properties.put("sku_id", finalObject1.sku_id);
//                                properties.put("product_id", finalObject1.product_id);
//                                properties.put("title", finalObject1.title);
//                                properties.put("discounted_price", finalObject1.discounted_price);
//                                properties.put("region_sale_price", finalObject1.region_sale_price);
//                                properties.put("brand", finalObject1.brand);
//                                properties.put("image_link", baseUrl + finalObject1.image_link);
//                                properties.put("link", finalObject1.link);
//                                properties.put("mad_id", finalObject1.mad_id);
//                                properties.put("sale_price", finalObject1.sale_price);
//                                properties.put("price", finalObject1.price);
//                                properties.put("uuid", finalObject1.uuid.toString());
//                                properties.put("parentCategory", finalObject1.parentCategory);
//                                properties.put("childCategories", childCategories.toArray());
//                                properties.put("color", finalObject1.color);
//                                dataObjs.add(properties);
////                    LOGGER.info("COMPLETED -> " + finalObject1.sku_id + " ");
//                            } catch (Exception ex) {
//                                LOGGER.error("ERROR -> " + finalObject1.sku_id + " " + ex.getMessage() + ex.getStackTrace());
//                            }
//                        }
//                    }
//                }
//                if (!dataObjs.isEmpty()) {
//                    this.pushToVectorDb(dataObjs);
//                }
//        } catch (Exception ex) {
//            LOGGER.error("ERROR -> " + ex.getMessage() + ex.getStackTrace());
//
//        }
    }

//    private KafkaPayload parentChildCategoryCorrection(RequestPayload productDetails) {
//        Set<ChildCategoryModel> allChildLabel = new HashSet<>();
//        String parentLabel = "NoParentFound";
//        for (CategoryModel aCategoryModel : productDetails.getParent_category()) {
//            if (null == aCategoryModel.label) {
//                continue;
//            }
//            String tempLabel = aCategoryModel.label.toLowerCase();
//            if (!PARENT_CATEGORY.contains(tempLabel)) {
//                ChildCategoryModel childCategoryModel = new ChildCategoryModel();
//                childCategoryModel.setLabel(tempLabel);
//                allChildLabel.add(childCategoryModel);
//            } else {
//                parentLabel = tempLabel;
//            }
//        }
//
//        ObjectMapper mapper = new ObjectMapper();
//        KafkaPayload aPayload = mapper.convertValue(productDetails, KafkaPayload.class);
//        aPayload.setParentCategory(parentLabel);
//        aPayload.setChildCategories(allChildLabel);
//        return aPayload;
//    }

    public void pushToVectorDb(List<Map<String, Object>> dataObjs) {
        if (dataObjs.size() > 0) {
            ObjectsBatcher batcher = singleWeaviateClient.weaviateClientMethod().batch().objectsBatcher();
            for (Map<String, Object> prop : dataObjs) {
                batcher.withObject(WeaviateObject.builder()
                        .className(className)
                        .properties(prop)
                        .id(prop.get("uuid").toString())
                        .build());
            }
            Result<ObjectGetResponse[]> a = batcher
                    .withConsistencyLevel(ConsistencyLevel.ONE)
                    .run();

            for (ObjectGetResponse b : a.getResult()) {
                if (!(b.getResult().toString().contains
                        ("SUCCESS"))) {

                    LOGGER.error("ERROR while bulk import -> " + b.getId());
                    LOGGER.error("ERROR " + b.getResult().toString());
                } else {
                    LOGGER.info("Completed bulk import -> " + b.getId());
                }
            }
        }
    }

    public String downloadAndDownSizeImage(String imageUrl) {
        try {
            URL url = new URL(imageUrl);
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream inputStream = httpConn.getInputStream();
                     ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

                    BufferedImage originalImage = ImageIO.read(inputStream);
                    int newWidth = (int) (originalImage.getWidth() * 0.45);
                    int newHeight = (int) (originalImage.getHeight() * 0.45);

                    BufferedImage resizedImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_BYTE_GRAY);
                    Graphics2D g = resizedImage.createGraphics();
                    g.drawImage(originalImage, 0, 0, newWidth, newHeight, null);
                    g.dispose();

                    // Convert the resized image to grayscale directly
                    Graphics2D g2d = resizedImage.createGraphics();
                    g2d.drawImage(resizedImage, 0, 0, null);
                    g2d.dispose();

                    ImageIO.write(resizedImage, "jpg", outputStream);
                    byte[] imageBytes = outputStream.toByteArray();
                    String base64Image = Base64.getEncoder().encodeToString(imageBytes).replace("\n", "").replace(" ", "");

                    System.out.println("Image downloaded, resized, and converted to grayscale successfully.");
                    return base64Image;
                }
            } else {
                System.out.println("Failed to download image. HTTP Error Code: " + responseCode);
            }
            httpConn.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public void gg(final List<KafkaPayload> allKafkaPayload) {
        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
        LOGGER.info("No of products -> " + allKafkaPayload.size());
        List<Map<String, Object>> dataObjs = new ArrayList<>();

        for (KafkaPayload kafkaPayload : allKafkaPayload) {
            if (kafkaPayload.base64Image == null || kafkaPayload.base64Image.length() < 10) {
                kafkaPayload.base64Image = this.downloadAndDownSizeImage(baseUrl + kafkaPayload.image_link);
            }

            Optional productDetailsOptional = imageRepo.findById(kafkaPayload.getEntity_id());
            if (productDetailsOptional.isPresent()) {
                KafkaPayload finalObject1 = this.updateProductDetailsToDb(kafkaPayload, productDetailsOptional);

                List<String> childCategories = new ArrayList<>();
                for (ChildCategoryModel a : finalObject1.child_categories) {
                    childCategories.add(a.getLabel());
                }

                Map<String, Object> properties = new HashMap<>();
                properties.put("image", finalObject1.base64Image);
                properties.put("sku_id", finalObject1.sku_id);
                properties.put("product_id", finalObject1.product_id);
                properties.put("brand", finalObject1.brand);
                properties.put("uuid", finalObject1.uuid.toString());
                properties.put("parentCategory", finalObject1.parent_category);
                properties.put("childCategories", childCategories.toArray(new String[0]));
                properties.put("color", finalObject1.color);

                dataObjs.add(properties);
            } else {
                LOGGER.info("No product details found for id " + kafkaPayload.entity_id);
            }
        }
        if(dataObjs.size() > 0){
            this.pushToVectorDb(dataObjs);
        }
    }

}