package com.huangshang.demo.center.dao;

import org.springframework.stereotype.Repository;

/**
 * Created by huangshang on 2018/8/25 下午4:47.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
@Repository
public interface MybatisTestMapper {
    Long insert(String name);
    String query(Long id);
}
