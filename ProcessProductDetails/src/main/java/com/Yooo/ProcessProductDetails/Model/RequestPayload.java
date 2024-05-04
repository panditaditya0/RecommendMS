package com.Yooo.ProcessProductDetails.Model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;

@Getter
@Setter
@ToString
public class RequestPayload {
    public long entity_id;
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
    public String uuid;
    public String color;
    public String domain;
    public ArrayList<CategoryModel> category;
}
