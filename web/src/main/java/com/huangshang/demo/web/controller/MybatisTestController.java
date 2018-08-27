package com.huangshang.demo.web.controller;

import com.huangshang.demo.center.dao.MybatisTestMapper;
import com.huangshang.demo.web.dto.CreateDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

/**
 * Created by huangshang on 2018/8/25 下午5:10.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
@RestController
@RequestMapping("/mybatis")
@Slf4j
public class MybatisTestController {

    @Autowired
    private MybatisTestMapper mybatisTestMapper;

    @RequestMapping(value = "/insert/{name}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Long testCreate(@PathVariable String name){
        try {
            Long id = mybatisTestMapper.insert(name);
            log.info("new insert mybatis test, return id = " + id);
            return id;
        }catch (Exception e){
            e.printStackTrace();
        }

        return -1L;
    }

    @RequestMapping(value = "/insert2", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Long testCreate(@RequestBody CreateDTO data){
        try {
            Long id = mybatisTestMapper.insert(data.getName());
            log.info("new insert mybatis test, return id = " + id);
            return id;
        }catch (Exception e){
            e.printStackTrace();
        }

        return -1L;
    }

    @RequestMapping(value = "/query/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String testCreate(@PathVariable Long id){
        try {
            String name = mybatisTestMapper.query(id);
            log.info("query id = {} , value = {} .", id, name);
            return name;
        }catch (Exception e){
            e.printStackTrace();
        }

        return "异常";
    }
}
