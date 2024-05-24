package com.Yooo.ProcessProductDetails.Services;

import com.Yooo.ProcessProductDetails.Config.SingleWeaviateClient;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Model.NewkafkaPayload;
import com.Yooo.ProcessProductDetails.Dto.RecommendCategoryDto;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.Yooo.ProcessProductDetails.Repo.NewImageRepo;
import com.google.gson.Gson;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class ProductDetailService {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);
    private final NewImageRepo newImageRepo;
    private final ImageRepo imageRepo;
    private final String className = "TestImg18";  // Replace with your class name
    private final SingleWeaviateClient singleWeaviateClient;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    private NewkafkaPayload updateProductDetailsToDb(NewkafkaPayload newData, Optional oldData) {
        try {
            NewkafkaPayload productDetailsFromSb = (NewkafkaPayload) oldData.get();
            newData.setUuid(productDetailsFromSb.getUuid());
            if(productDetailsFromSb.base64Image != null && productDetailsFromSb.base64Image.length() >10) {
                newData.base64Image = productDetailsFromSb.base64Image;
            }
        } catch (Exception ex) {
           throw new RuntimeException("Error updating product details"+newData.entity_id+" " + ex.getMessage());
        }
        return newImageRepo.save(newData);
    }
    private NewkafkaPayload saveImageDetailsToDb(NewkafkaPayload productDetails) {
        try {
            productDetails.setUuid(UUID.randomUUID().toString());
            newImageRepo.save(productDetails);
            LOGGER.info("NEW ENTRY " + productDetails.getEntity_id());
        } catch (Exception ex) {
            LOGGER.error("ERROR FOR " + productDetails.getEntity_id() + ex.getMessage() + ex.getStackTrace());
        }
        return productDetails;
    }

    public void processProductDetails(List<HashMap<String, Object>> allProductDetails) {
        ArrayList<NewkafkaPayload> allProductsDetails = new ArrayList<>();
        try {

            for (HashMap<String, Object> map : allProductDetails) {
                Gson gson = new Gson();
                String categories = gson.toJson(map.get("parent_child_categories"));
                KafkaPayload fromDbOptional= imageRepo.findById(Long.parseLong((String) map.get("entity_id"))).get();
                NewkafkaPayload payload = new NewkafkaPayload();
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
                payload.setIn_stock((String) map.get("in_stock"));
                payload.setUpdated_at(LocalDateTime.parse(LocalDateTime.now().format(dateTimeFormatter), dateTimeFormatter));
                payload.setCategories((String) categories);
                payload.setBase64Image(fromDbOptional.base64Image);
                payload.setParent_categories((List<String>) map.get("parent_categories"));
                payload.setChild_categories((List<String>) map.get("child_categories"));
                allProductsDetails.add(payload);

                Optional productDetailsOptional = newImageRepo.findById(payload.getEntity_id());
                NewkafkaPayload finalObject = new NewkafkaPayload();
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
    }

    public void pushToVectorDb(List<Map<String, Object>> dataObjs) {
        if (dataObjs.size() > 0) {
            ObjectsBatcher batcher = singleWeaviateClient.weaviateClientMethod().batch().objectsBatcher();
            for (Map<String, Object> prop : dataObjs) {
                String i_d = prop.get("some_i").toString();
                prop.remove("some_i");
                batcher.withObject(WeaviateObject.builder()
                        .className(className)
                        .properties(prop)
                        .id(i_d)
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
        HttpURLConnection httpConn=null;
        try {
            URL url = new URL(imageUrl);
             httpConn = (HttpURLConnection) url.openConnection();
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
                    outputStream.flush();
                    byte[] imageBytes = outputStream.toByteArray();
                    outputStream.close();

                    String base64Image = Base64.getEncoder().encodeToString(imageBytes).replace("\n", "").replace(" ", "");

                    System.out.println("Image downloaded, resized, and converted to grayscale successfully.");
                    return base64Image;
                }
            } else {
                System.out.println("Failed to download image. HTTP Error Code: " + responseCode);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(httpConn!=null) {
                httpConn.disconnect();
            }
        }
        return "";
    }

    public List<Map<String, Object>> gg(final List<NewkafkaPayload> allKafkaPayload) {
        String baseUrl = "https://img.perniaspopupshop.com/catalog/product";
        LOGGER.info("No of products -> " + allKafkaPayload.size());
        List<Map<String, Object>> dataObjs = new ArrayList<>();

        for (NewkafkaPayload kafkaPayload : allKafkaPayload) {
            if (kafkaPayload.base64Image == null || kafkaPayload.base64Image.length() < 10) {
                kafkaPayload.base64Image = this.downloadAndDownSizeImage(baseUrl + kafkaPayload.image_link);
            }

            Gson gson = new Gson();

            Optional productDetailsOptional = newImageRepo.findById(kafkaPayload.getEntity_id());
            if (productDetailsOptional.isPresent()) {
                NewkafkaPayload finalObject1 = this.updateProductDetailsToDb(kafkaPayload, productDetailsOptional);
                ArrayList<RecommendCategoryDto> a = gson.fromJson(finalObject1.categories, ArrayList.class);
                Map<String, Object> properties = new HashMap<>();
                properties.put("image", finalObject1.base64Image);
                properties.put("sku_id", finalObject1.sku_id);
                properties.put("product_id", finalObject1.product_id);
                properties.put("brand", finalObject1.brand);
                properties.put("some_i", finalObject1.uuid.toString());
                properties.put("color", finalObject1.color);
                properties.put("in_stock",finalObject1.in_stock);
                properties.put("parent_categories", finalObject1.parent_categories);
                properties.put("child_categories", finalObject1.child_categories);
                dataObjs.add(properties);
            } else {
                LOGGER.info("No product details found for id " + kafkaPayload.entity_id);
            }
        }
      return dataObjs;
    }
}