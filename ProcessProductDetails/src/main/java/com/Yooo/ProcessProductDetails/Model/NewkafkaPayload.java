package com.Yooo.ProcessProductDetails.Model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Entity
@Table(name = "product_details_3")
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class NewkafkaPayload {
    @Id
    public long entity_id;
    public String sku_id;
    public String product_id;
    public String title;
    public String brand;
    public String image_link;
    public double discount;
    public String link;
    public String color;
    public String domain;
    public String parent_category;
    public double price_in;
    public double discount_in;
    public double special_price_in;
    public double price_us;
    public double discount_us;
    public double special_price_us;
    public double price_row;
    public double discount_row;
    public double special_price_row;
    public String uuid;
    public LocalDateTime updated_at;
    @Column(name = "base_64_image")
    public String base64Image;
    public String categories;
    public String in_stock;
}