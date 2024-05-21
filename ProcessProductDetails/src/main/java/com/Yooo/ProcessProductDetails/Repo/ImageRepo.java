package com.Yooo.ProcessProductDetails.Repo;

import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public interface ImageRepo extends JpaRepository<KafkaPayload, Long> {

    @Query(value="select * from product_details_2 pd where brand = ?1",  nativeQuery = true)
    List<KafkaPayload> findByBrand(String value);

    @Query(value="select sku_id from product_details_2 pd  where pd.base_64_image is not null",  nativeQuery = true)
    ArrayList<String> findByParent();

    @Query(value = "select sku_id from product_details_2 where brand = ?1 and base_64_image is not null  ",  nativeQuery = true)
    ArrayList<String> listOfSkuId( String value);

    @Query(value = "select * from product_details_2",  nativeQuery = true)
    ArrayList<KafkaPayload> listOfAllProducts();

    @Query(value = "select sku_id from product_details_2",  nativeQuery = true)
    ArrayList<String> listOfAllSkuIds();

    @Query(value = "select * from product_details_2 where sku_id in (?1)",  nativeQuery = true)
    ArrayList<KafkaPayload> getListOfProducts(List<String> sublist);
}