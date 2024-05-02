package com.Yooo.ProcessProductDetails.Model;

import java.util.Objects;
import java.util.stream.Stream;

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

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getSku_id() {
        return sku_id;
    }

    public void setSku_id(String sku_id) {
        this.sku_id = sku_id;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getDiscounted_price() {
        return discounted_price;
    }

    public void setDiscounted_price(double discounted_price) {
        this.discounted_price = discounted_price;
    }

    public double getRegion_sale_price() {
        return region_sale_price;
    }

    public void setRegion_sale_price(double region_sale_price) {
        this.region_sale_price = region_sale_price;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getImage_link() {
        return image_link;
    }

    public void setImage_link(String image_link) {
        this.image_link = image_link;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getMad_id() {
        return mad_id;
    }

    public void setMad_id(String mad_id) {
        this.mad_id = mad_id;
    }

    public double getSale_price() {
        return sale_price;
    }

    public void setSale_price(double sale_price) {
        this.sale_price = sale_price;
    }

    public String getOntology() {
        return ontology;
    }

    public void setOntology(String ontology) {
        this.ontology = ontology;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getEntity_id() {
        return entity_id;
    }

    public void setEntity_id(String entity_id) {
        this.entity_id = entity_id;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public boolean checkAnyNull(){
        return Stream.of(sku_id,product_id
                        ,title, discounted_price, region_sale_price
                        ,brand,image_link, discount, link, mad_id
                        , sale_price, ontology,price, color)
                .anyMatch(Objects::isNull);
    }
}
