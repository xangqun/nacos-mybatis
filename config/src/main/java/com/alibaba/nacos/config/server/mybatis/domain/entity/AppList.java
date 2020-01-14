package com.alibaba.nacos.config.server.mybatis.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
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
@TableName("APP_LIST")
@ApiModel(value = "AppList对象", description = "")
public class AppList extends Model<AppList> {

    public static final String ID = "ID";
    public static final String APP_NAME = "APP_NAME";
    public static final String IS_DYNAMIC_COLLECT_DISABLED = "IS_DYNAMIC_COLLECT_DISABLED";
    public static final String LAST_SUB_INFO_COLLECTED_TIME = "LAST_SUB_INFO_COLLECTED_TIME";
    public static final String SUB_INFO_LOCK_OWNER = "SUB_INFO_LOCK_OWNER";
    public static final String SUB_INFO_LOCK_TIME = "SUB_INFO_LOCK_TIME";
    private static final long serialVersionUID = 1L;
    @TableId(value = "ID", type = IdType.ID_WORKER)
    private Long id;
    @TableField("APP_NAME")
    private String appName;
    @TableField("IS_DYNAMIC_COLLECT_DISABLED")
    private Double isDynamicCollectDisabled;
    @TableField("LAST_SUB_INFO_COLLECTED_TIME")
    private Date lastSubInfoCollectedTime;
    @TableField("SUB_INFO_LOCK_OWNER")
    private String subInfoLockOwner;
    @TableField("SUB_INFO_LOCK_TIME")
    private Date subInfoLockTime;

    @Override
    protected Serializable pkVal() {
        return this.id;
    }

}
