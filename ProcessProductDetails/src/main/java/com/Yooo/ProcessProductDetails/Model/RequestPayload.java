package com.Yooo.ProcessProductDetails.Model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;

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
}
