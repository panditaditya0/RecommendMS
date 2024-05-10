package com.Yooo.ProcessProductDetails.Services;

import com.Yooo.ProcessProductDetails.Config.SingleWeaviateClient;
import com.Yooo.ProcessProductDetails.Model.CategoryModel;
import com.Yooo.ProcessProductDetails.Model.ChildCategoryModel;
import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ProductDetailService {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);
    public ImageRepo imageRepo;
    public final String className = "TestImg16";  // Replace with your class name
    public static Set<String> PARENT_CATEGORY = new HashSet<>(Arrays.asList("lehenga", "sarees", "gown", "dresses", "kurta set", "jacket", "jumpsuits", "sharara set", "co-ord set", "designer ghararas", "resort and beach wear", "bralettes for women", "kurtas for women", "designer anarkali", "jackets for women", "pants", "kaftans", "tops", "skirts"));
    private SingleWeaviateClient singleWeaviateClient;

    public ProductDetailService(ImageRepo imageRep, SingleWeaviateClient singleWeaviateClient) {
        this.imageRepo = imageRep;
        this.singleWeaviateClient = singleWeaviateClient;
    }

    private KafkaPayload updateProductDetailsToDb(KafkaPayload productDetails) {
        try {
            Optional op = imageRepo.findById(productDetails.getEntity_id());
            if (op.isPresent()) {
                KafkaPayload productDetailsFromSb = (KafkaPayload) op.get();
                productDetails.setUuid(productDetailsFromSb.getUuid());
            } else {
                LOGGER.error("ERROR UPDATING " + productDetails.getEntity_id());
            }
        } catch (Exception ex) {
            LOGGER.error("ERROR UPDATING " + productDetails.getEntity_id());
        }
//        LOGGER.info("UPDATED IN DB "+ productDetails.getEntity_id());
        return imageRepo.save(productDetails);
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

    public void processProductDetails(List<RequestPayload> allProductDetails) {
        List<Map<String, Object>> dataObjs = new ArrayList<>();
        List<RequestPayload> newAllProductDetails = allProductDetails.stream()
                .filter(product -> product != null && product.getSku_id() != null)
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(RequestPayload::getSku_id, p -> p, (p1, p2) -> p1), // Use sku as key for distinct
                        m -> new ArrayList<>(m.values()))); // Convert map values to list


        try {
            List<List<RequestPayload>> inputChunk = Lists.partition(newAllProductDetails, 3);
            for (List<RequestPayload> chunk : inputChunk) {
                for (RequestPayload productDetails : chunk) {
                    KafkaPayload aKafkaProductPayload = this.parentChildCategoryCorrection(productDetails);
                    Optional productDetailsOptional = imageRepo.findById(aKafkaProductPayload.getEntity_id());
                    KafkaPayload finalObject = aKafkaProductPayload;
                    if (productDetailsOptional.isEmpty()) {
                        finalObject = this.saveImageDetailsToDb(aKafkaProductPayload);
                    } else {
                        finalObject = this.updateProductDetailsToDb(aKafkaProductPayload);
                    }

                    KafkaPayload finalObject1 = finalObject;
                    if (true) {
                        if (true) {
//                        String baseUrl = System.getenv("download_image_base_url");
//                            String baseUrl = "https://dimension-six.perniaspopupshop.com/media/catalog/product";
                            String baseUrl = "https://img.perniaspopupshop.com/catalog/product";

                            try {
                                String base64Image = downloadAndDownSizeImage(baseUrl+ finalObject1.image_link);
                                List<String> childCategories = new ArrayList<>();
                                Iterator it = finalObject1.childCategories.iterator();
                                while (it.hasNext()) {
                                    ChildCategoryModel a = (ChildCategoryModel) it.next();
                                    childCategories.add(a.getLabel());
                                }

                                Map<String, Object> properties = new HashMap<>();
                                properties.put("image", base64Image);
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
//                    LOGGER.info("COMPLETED -> " + finalObject1.sku_id + " ");
                            } catch (Exception ex) {
                                LOGGER.error("ERROR -> " + finalObject1.sku_id + " " + ex.getMessage() + ex.getStackTrace());
                            }
                        }
                    }
                }
                if (!dataObjs.isEmpty()) {
                    this.pushToVectorDb(dataObjs);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("ERROR -> " + ex.getMessage() + ex.getStackTrace());

        }
    }

    private KafkaPayload parentChildCategoryCorrection(RequestPayload productDetails) {
        Set<ChildCategoryModel> allChildLabel = new HashSet<>();
        String parentLabel = "NoParentFound";
        for (CategoryModel aCategoryModel : productDetails.getCategory()) {
            if (null == aCategoryModel.label) {
                continue;
            }
            String tempLabel = aCategoryModel.label.toLowerCase();
            if (!PARENT_CATEGORY.contains(tempLabel)) {
                ChildCategoryModel childCategoryModel = new ChildCategoryModel();
                childCategoryModel.setLabel(tempLabel);
                allChildLabel.add(childCategoryModel);
            } else {
                parentLabel = tempLabel;
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        KafkaPayload aPayload = mapper.convertValue(productDetails, KafkaPayload.class);
        aPayload.setParentCategory(parentLabel);
        aPayload.setChildCategories(allChildLabel);
        return aPayload;
    }

    private void pushToVectorDb(List<Map<String, Object>> dataObjs) {
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
                if (!(b.getResult().toString().contains("ObjectsGetResponseAO2Result(errors=null, status=null)"))) {

                    LOGGER.error("ERROR while bulk import -> " + b.getId());
                    LOGGER.error("ERROR " + b.getResult().toString());
                } else {
                    LOGGER.info("Completed bulk import -> " + b.getId());
                }
            }
        }
    }

    private String downloadAndDownSizeImage(String imageUrl){
        try {
            URL url = new URL(imageUrl);
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = httpConn.getInputStream();
                BufferedImage originalImage = ImageIO.read(inputStream);
                int newWidth = (int) (originalImage.getWidth() * 0.35);
                int newHeight = (int) (originalImage.getHeight() * 0.35);
                BufferedImage resizedImage = new BufferedImage(newWidth, newHeight, originalImage.getType());
                resizedImage.createGraphics().drawImage(originalImage, 0, 0, newWidth, newHeight, null);

                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ImageIO.write(resizedImage, "jpg", outputStream);
                byte[] imageBytes = outputStream.toByteArray();
                String base64Image = Base64.getEncoder().encodeToString(imageBytes);

                base64Image.replace("\n", "").replace(" ", "");
                inputStream.close();
                outputStream.close();

                System.out.println("Image downloaded and resized successfully.");
                return base64Image;
            } else {
                System.out.println("Failed to download image. HTTP Error Code: " + responseCode);
            }

            httpConn.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}