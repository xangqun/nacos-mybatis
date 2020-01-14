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
@TableName("USERS")
@ApiModel(value = "Users对象", description = "")
public class Users extends Model<Users> {

    public static final String USERNAME = "USERNAME";
    public static final String PASSWORD = "PASSWORD";
    public static final String ENABLED = "ENABLED";
    private static final long serialVersionUID = 1L;
    @TableId(value = "USERNAME", type = IdType.ID_WORKER)
    private String username;
    @TableField("PASSWORD")
    private String password;
    @TableField("ENABLED")
    private Integer enabled;

    @Override
    protected Serializable pkVal() {
        return this.username;
    }

}
