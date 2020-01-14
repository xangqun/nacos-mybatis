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
@TableName("CONFIG_TAGS_RELATION")
@ApiModel(value = "ConfigTagsRelation对象", description = "")
public class ConfigTagsRelation extends Model<ConfigTagsRelation> {

    public static final String ID = "ID";
    public static final String TAG_NAME = "TAG_NAME";
    public static final String TAG_TYPE = "TAG_TYPE";
    public static final String DATA_ID = "DATA_ID";
    public static final String GROUP_ID = "GROUP_ID";
    public static final String TENANT_ID = "TENANT_ID";
    public static final String NID = "NID";
    private static final long serialVersionUID = 1L;
    @TableField("ID")
    private Long id;
    @TableField("TAG_NAME")
    private String tagName;
    @TableField("TAG_TYPE")
    private String tagType;
    @TableField("DATA_ID")
    private String dataId;
    @TableField("GROUP_ID")
    @JsonProperty("group")
    private String groupId;
    @TableField("TENANT_ID")
    @JsonProperty("tenant")
    private String tenantId;
    @TableId(value = "NID", type = IdType.ID_WORKER)
    private Long nid;

    @Override
    protected Serializable pkVal() {
        return this.nid;
    }

}
