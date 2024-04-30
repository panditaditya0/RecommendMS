package com.Yooo.ProcessProductDetails.Repo;

import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ImageRepo extends JpaRepository<RequestPayload, Long> {
}
