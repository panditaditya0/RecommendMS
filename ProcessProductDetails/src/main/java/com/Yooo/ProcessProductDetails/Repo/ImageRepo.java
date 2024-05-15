package com.Yooo.ProcessProductDetails.Repo;

import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ImageRepo extends JpaRepository<KafkaPayload, Long> {

    @Query(value="select * from product_details_2 pd where brand = ?1",  nativeQuery = true)
    List<KafkaPayload> findByBrand(String brand);
}