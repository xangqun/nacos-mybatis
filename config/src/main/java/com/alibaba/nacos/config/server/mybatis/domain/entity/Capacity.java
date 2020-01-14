/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.config.server.mybatis.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.util.Date;

/**
 * Capacity
 *
 * @author hexu.hxy
 * @date 2018/3/13
 */
@Data
public class Capacity {
    public static final String ID = "ID";
    public static final String QUOTA = "QUOTA";
    public static final String USAGE = "USAGE";
    public static final String MAX_SIZE = "MAX_SIZE";
    public static final String MAX_AGGR_COUNT = "MAX_AGGR_COUNT";
    public static final String MAX_AGGR_SIZE = "MAX_AGGR_SIZE";
    public static final String MAX_HISTORY_COUNT = "MAX_HISTORY_COUNT";
    public static final String GMT_CREATE = "GMT_CREATE";
    public static final String GMT_MODIFIED = "GMT_MODIFIED";
    private static final long serialVersionUID = 1L;
    @TableId(value = "ID", type = IdType.ID_WORKER)
    private Long id;
    @TableField("QUOTA")
    private Integer quota;
    @TableField("USAGE")
    private Integer usage;
    @TableField("MAX_SIZE")
    private Integer maxSize;
    @TableField("MAX_AGGR_COUNT")
    private Integer maxAggrCount;
    @TableField("MAX_AGGR_SIZE")
    private Integer maxAggrSize;
    @TableField("MAX_HISTORY_COUNT")
    private Integer maxHistoryCount;
    @TableField("GMT_CREATE")
    private Date gmtCreate;
    @TableField("GMT_MODIFIED")
    private Date gmtModified;

    public Date getGmtCreate() {
        if (gmtCreate == null) {
            return null;
        }
        return new Date(gmtCreate.getTime());
    }

    public void setGmtCreate(Date gmtCreate) {
        if (gmtCreate == null) {
            this.gmtCreate = null;
        } else {
            this.gmtCreate = new Date(gmtCreate.getTime());
        }

    }

    public Date getGmtModified() {
        if (gmtModified == null) {
            return null;
        }
        return new Date(gmtModified.getTime());
    }

    public void setGmtModified(Date gmtModified) {
        if (gmtModified == null) {
            this.gmtModified = null;
        } else {
            this.gmtModified = new Date(gmtModified.getTime());
        }
    }
}
