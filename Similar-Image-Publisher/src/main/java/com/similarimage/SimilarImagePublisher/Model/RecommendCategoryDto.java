package com.similarimage.SimilarImagePublisher.Model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;

@Getter
@Setter
public class RecommendCategoryDto {
    public String parent_category;
    public ArrayList<String> child_categories;
}
