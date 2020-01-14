package com.alibaba.nacos.config.server.mybatis.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
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
@TableName("CONFIG_INFO_BETA")
@ApiModel(value = "ConfigInfoBeta对象", description = "")
public class ConfigInfoBeta extends Model<ConfigInfoBeta> {

    public static final String ID = "ID";
    public static final String DATA_ID = "DATA_ID";
    public static final String GROUP_ID = "GROUP_ID";
    public static final String APP_NAME = "APP_NAME";
    public static final String BETA_IPS = "BETA_IPS";
    public static final String MD5 = "MD5";
    public static final String GMT_CREATE = "GMT_CREATE";
    public static final String GMT_MODIFIED = "GMT_MODIFIED";
    public static final String SRC_IP = "SRC_IP";
    public static final String TENANT_ID = "TENANT_ID";
    public static final String SRC_USER = "SRC_USER";
    public static final String CONTENT = "CONTENT";
    private static final long serialVersionUID = 1L;
    @TableId(value = "ID", type = IdType.ID_WORKER)
    private Long id;
    @TableField("DATA_ID")
    private String dataId;
    @TableField("GROUP_ID")
    @JsonProperty("group")
    private String groupId;
    @TableField("APP_NAME")
    private String appName;
    @TableField("BETA_IPS")
    private String betaIps;
    @TableField("MD5")
    private String md5;
    @TableField("GMT_CREATE")
    private Date gmtCreate;
    @TableField("GMT_MODIFIED")
    private Date gmtModified;
    @TableField("SRC_IP")
    private String srcIp;
    @TableField("TENANT_ID")
    @JsonProperty("tenant")
    private String tenantId;
    @TableField("SRC_USER")
    private String srcUser;
    @TableField("CONTENT")
    private String content;

    @Override
    protected Serializable pkVal() {
        return this.id;
    }

}
