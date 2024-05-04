package com.Yooo.ProcessProductDetails.Repo;

import com.Yooo.ProcessProductDetails.Model.KafkaPayload;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ImageRepo extends JpaRepository<KafkaPayload, Long> {
}
