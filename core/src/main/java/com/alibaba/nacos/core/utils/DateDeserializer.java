package com.alibaba.nacos.core.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>
 * 时间反序列化，按照用户首选项设置的时区对时间进行转换
 * 如果属性被标记为忽略时区，则不做时区转换
 * </p>
 *
 * @author qingsheng.chen 2018/8/27 星期一 9:57
 * @see IgnoreTimeZone
 */
public class DateDeserializer extends JsonDeserializer<Date> implements ContextualDeserializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateDeserializer.class);
    private boolean ignoreTimeZone = false;
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public Date deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        try {
            SimpleDateFormat dateFormatGmt = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
            if (ignoreTimeZone) {
                return dateFormatGmt.parse(jsonParser.getValueAsString());
            }
            return dateFormatGmt.parse(jsonParser.getValueAsString());
        } catch (Exception e) {
            LOGGER.warn("date format error : {}", e);
            return new Date(jsonParser.getValueAsLong());
        }
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
        return new DateDeserializer().setIgnoreTimeZone(true);
    }

    private DateDeserializer setIgnoreTimeZone(boolean ignoreTimeZone) {
        this.ignoreTimeZone = ignoreTimeZone;
        return this;
    }
}
