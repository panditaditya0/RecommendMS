package com.similarimage.SimilarImagePublisher.Model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.*;
import java.util.stream.Stream;

@Getter
@Setter
@ToString
public class RequestPayload {
    public String sku_id;
    public String product_id;
    public String title;
    public String brand;
    public String image_link;
    public float discount;
    public String link;
    public String entity_id;
    public String color;
    public String domain;
    public String parent_category;
    public ArrayList<String> child_categories;
    public float price_in;
    public float discount_in;
    public float special_price_in;
    public float price_us;
    public float discount_us;
    public float special_price_us;
    public float price_row;
    public float discount_row;
    public float special_price_row;
    public String in_stock;
    public HashMap<String, ArrayList<String>> parent_child_categories;

    public boolean checkAnyNull(){
        return Stream.of(sku_id,product_id
                        ,title,brand,image_link, image_link, discount, link
                        , entity_id, color,domain, parent_category,child_categories
                ,price_in,discount_in,special_price_in,price_us,discount_us,special_price_us
                ,price_row,discount_row,special_price_row,parent_child_categories)
                .anyMatch(Objects::isNull);
    }
}
