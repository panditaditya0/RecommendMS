package com.Yooo.ProcessProductDetails.Controller;

import com.Yooo.ProcessProductDetails.Model.NewkafkaPayload;
import com.Yooo.ProcessProductDetails.Repo.ImageRepo;
import com.Yooo.ProcessProductDetails.Repo.NewImageRepo;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequiredArgsConstructor
@Slf4j
public class PushController {
    public final ImageRepo imageRepo;
    public final NewImageRepo newImageRepo;
    public final ProductDetailService productDetailService;
    private RedisTemplate template;

    @CrossOrigin
    @GetMapping("/push/allImages")
    public ResponseEntity pushAllImages(){
        List<Long> allSkuIds = newImageRepo.pushSomeImages();
        Collections.reverse(allSkuIds);
        List<List<Long>> chunks = Lists.partition(allSkuIds, 25);
        int counter = 0;
        for (List<Long> sublist : chunks) {
            try{
                ArrayList<NewkafkaPayload> listOfKafkaProducts = newImageRepo.getListOfProducts(sublist);
//                final List<Map<String, Object>> listOfProps = productDetailService.gg(listOfKafkaProducts);
//                productDetailService.pushToVectorDb(listOfProps);
//                counter +=listOfProps.size();
                log.info("Pushed ->  "+ counter);
                System.gc();
                Thread.sleep(2);
            } catch (Exception ex){
                log.error(ex.getMessage());
            }

        }
        return ResponseEntity.ok().build();
    }
}

