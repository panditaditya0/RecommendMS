package com.Yooo.ProcessProductDetails.Repo;

import com.Yooo.ProcessProductDetails.Model.NewkafkaPayload;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public interface NewImageRepo extends JpaRepository<NewkafkaPayload, Long> {
    @Query(value="select sku_id from product_details_3 pd where base_64_image is not null",  nativeQuery = true)
    ArrayList<String> findByParent();

    @Query(value = "select * from product_details_3 where entity_id in (?1)",  nativeQuery = true)
    ArrayList<NewkafkaPayload> getListOfProducts(List<Long> sublist);

    @Query(value="select entity_id  from product_details_3 pd  where base_64_image is not null and categories is not null\n",  nativeQuery = true)
    ArrayList<Long> pushSomeImages();
}