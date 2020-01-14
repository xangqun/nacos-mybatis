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
@TableName("APP_CONFIGDATA_RELATION_SUBS")
@ApiModel(value = "AppConfigdataRelationSubs对象", description = "")
public class AppConfigdataRelationSubs extends Model<AppConfigdataRelationSubs> {

    public static final String ID = "ID";
    public static final String APP_NAME = "APP_NAME";
    public static final String DATA_ID = "DATA_ID";
    public static final String GROUP_ID = "GROUP_ID";
    public static final String GMT_MODIFIED = "GMT_MODIFIED";
    private static final long serialVersionUID = 1L;
    @TableId(value = "ID", type = IdType.ID_WORKER)
    private Long id;
    @TableField("APP_NAME")
    private String appName;
    @TableField("DATA_ID")
    private String dataId;
    @JsonProperty("group")
    @TableField("GROUP_ID")
    private String groupId;
    @TableField("GMT_MODIFIED")
    private Date gmtModified;

    @Override
    protected Serializable pkVal() {
        return this.id;
    }

}
