package com.alibaba.nacos.config.server.mybatis.domain.entity;

import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * <p>
 *
 * </p>
 *
 * @author xangqun
 * @since 2019-07-01
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("CONFIG_INFO")
@ApiModel(value = "ConfigInfo对象", description = "")
//@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigInfo extends ConfigInfoBase {

    public static final String DATA_ID = "DATA_ID";
    public static final String GROUP_ID = "GROUP_ID";
    public static final String MD5 = "MD5";
    public static final String GMT_CREATE = "GMT_CREATE";
    public static final String GMT_MODIFIED = "GMT_MODIFIED";
    public static final String SRC_IP = "SRC_IP";
    public static final String APP_NAME = "APP_NAME";
    public static final String TENANT_ID = "TENANT_ID";
    public static final String C_DESC = "C_DESC";
    public static final String C_USE = "C_USE";
    public static final String EFFECT = "EFFECT";
    public static final String TYPE = "TYPE";
    public static final String ID = "ID";
    public static final String SRC_USER = "SRC_USER";
    public static final String CONTENT = "CONTENT";
    public static final String C_SCHEMA = "C_SCHEMA";
    private static final long serialVersionUID = 1L;

    @TableField("GMT_CREATE")
    private Date gmtCreate;
    @TableField("GMT_MODIFIED")
    private Date gmtModified;
    @TableField("SRC_IP")
    private String srcIp;
    @TableField("APP_NAME")
    private String appName;
    @JsonProperty("tenant")
    @TableField("TENANT_ID")
    private String tenantId;
    @TableField("C_DESC")
    @JsonProperty("desc")
    private String cDesc;
    @TableField("C_USE")
    private String cUse;
    @TableField("EFFECT")
    private String effect;
    @TableField("TYPE")
    private String type;
    @TableField("SRC_USER")
    private String srcUser;
    @TableField("C_SCHEMA")
    private String cSchema;


    public ConfigInfo() {

    }

    public ConfigInfo(String dataId, String group, String content) {
        super(dataId, group, content);
    }

    public ConfigInfo(String dataId, String group, String appName, String content) {
        super(dataId, group, content);
        this.appName = appName;
    }

    public ConfigInfo(String dataId, String group, String tenant, String appName, String content) {
        super(dataId, group, content);
        this.tenantId = tenant;
        this.appName = appName;
    }


}
