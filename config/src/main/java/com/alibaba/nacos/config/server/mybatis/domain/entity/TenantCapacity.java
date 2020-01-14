package com.alibaba.nacos.config.server.mybatis.domain.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

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
@TableName("TENANT_CAPACITY")
@ApiModel(value = "TenantCapacity对象", description = "")
public class TenantCapacity extends Capacity {

    public static final String TENANT_ID = "TENANT_ID";

    @TableField("TENANT_ID")
    @JsonProperty("tenant")
    private String tenantId;
}
