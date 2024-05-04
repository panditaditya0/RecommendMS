package com.similarimage.SimilarImagePublisher.Model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

@Getter
@Setter
@ToString
public class RequestPayload {
    public String sku_id;
    public String product_id;
    public String title;
    public double discounted_price;
    public double region_sale_price;
    public String brand;
    public String image_link;
    public double discount;
    public String link;
    public String mad_id;
    public double sale_price;
    public String ontology;
    public double price;
    public String entity_id;
    public String uuid;
    public String color;
    public ArrayList<CategoryModel> category;
    public String domain;

    public boolean checkAnyNull(){
        return Stream.of(sku_id,product_id
                        ,title, discounted_price, region_sale_price
                        ,brand,image_link, discount, link, mad_id
                        , sale_price, ontology,price, color)
                .anyMatch(Objects::isNull);
    }
}
