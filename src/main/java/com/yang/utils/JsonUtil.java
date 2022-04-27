package com.yang.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import org.apache.commons.lang3.StringUtils;


/**
 * @author zhangyang03
 * @Description JsonUtil 公共方法
 * @create 2022-04-27 11:59
 */
public class JsonUtil {
    private JsonUtil() {
    }

    public static boolean isJson(String jsonStr) {
        if (StringUtils.isBlank(jsonStr)) {
            return false;
        }
        try {
            JSON.parse(jsonStr);
            return true;
        } catch (JSONException e) {
            return false;
        }
    }

}

