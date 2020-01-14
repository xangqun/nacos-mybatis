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
@TableName("TENANT_INFO")
@ApiModel(value = "TenantInfo对象", description = "")
public class TenantInfo extends Model<TenantInfo> {

    public static final String ID = "ID";
    public static final String KP = "KP";
    public static final String TENANT_ID = "TENANT_ID";
    public static final String TENANT_NAME = "TENANT_NAME";
    public static final String TENANT_DESC = "TENANT_DESC";
    public static final String CREATE_SOURCE = "CREATE_SOURCE";
    public static final String GMT_CREATE = "GMT_CREATE";
    public static final String GMT_MODIFIED = "GMT_MODIFIED";
    private static final long serialVersionUID = 1L;
    @TableId(value = "ID", type = IdType.ID_WORKER)
    private Long id;
    @TableField("KP")
    private String kp;
    @TableField("TENANT_ID")
    @JsonProperty("tenant")
    private String tenantId;
    @TableField("TENANT_NAME")
    private String tenantName;
    @TableField("TENANT_DESC")
    private String tenantDesc;
    @TableField("CREATE_SOURCE")
    private String createSource;
    @TableField("GMT_CREATE")
    private Long gmtCreate;
    @TableField("GMT_MODIFIED")
    private Long gmtModified;

    @Override
    protected Serializable pkVal() {
        return this.id;
    }

}
