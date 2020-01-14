package com.alibaba.nacos.core.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;

import java.io.IOException;
import java.util.Date;

/**
 * <p>
 * 时间序列化，按照用户首选项设置的时区对时间进行转换
 * 如果属性被标记为忽略时区，则不做时区转换
 *
 * @author qingsheng.chen 2018/8/27 星期一 9:27
 * @see IgnoreTimeZone
 * </p>
 */
public class DateSerializer extends JsonSerializer<Date> implements ContextualSerializer {
    private boolean ignoreTimeZone = false;
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void serialize(Date date, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        if (ignoreTimeZone) {
            jsonGenerator.writeNumber(date.getTime());
        }
    }


    private DateSerializer setIgnoreTimeZone(boolean ignoreTimeZone) {
        this.ignoreTimeZone = ignoreTimeZone;
        return this;
    }

    @Override
    public JsonSerializer<?> createContextual(SerializerProvider prov, BeanProperty property) {
        return new DateSerializer().setIgnoreTimeZone(true);
    }
}
