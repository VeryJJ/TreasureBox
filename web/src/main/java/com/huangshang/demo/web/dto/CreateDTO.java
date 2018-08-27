package com.huangshang.demo.web.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by huangshang on 2018/8/25 下午6:19.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
@Data
public class CreateDTO implements Serializable {

    private static final long serialVersionUID = 1292963198817605247L;
    private Long id;
    private String name;
}
