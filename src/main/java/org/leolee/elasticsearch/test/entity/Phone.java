package org.leolee.elasticsearch.test.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName Phone
 * @Description: TODO
 * @Author LeoLee
 * @Date 2021/6/7
 * @Version V1.0
 **/
@Data
@AllArgsConstructor
public class Phone {

    private String name;

    private String brand;

    private int price;
}
