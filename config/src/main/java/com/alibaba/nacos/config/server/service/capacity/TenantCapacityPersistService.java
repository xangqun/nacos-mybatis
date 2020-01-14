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
package com.alibaba.nacos.config.server.service.capacity;

import com.alibaba.nacos.config.server.mybatis.domain.entity.ConfigInfo;
import com.alibaba.nacos.config.server.mybatis.domain.entity.TenantCapacity;
import com.alibaba.nacos.config.server.mybatis.service.ConfigInfoService;
import com.alibaba.nacos.config.server.mybatis.service.TenantCapacityService;

import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.List;


/**
 * Tenant Capacity Service
 *
 * @author hexu.hxy
 * @date 2018/03/05
 */
@Service
public class TenantCapacityPersistService {

    @Autowired
    private TenantCapacityService tenantCapacityService;
    @Autowired
    private ConfigInfoService configInfoService;


    public TenantCapacity getTenantCapacity(String tenantId) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantId);
        List<TenantCapacity> tenantCapacityList = tenantCapacityService.list(new QueryWrapper<>(tenantCapacityParam));
        if (tenantCapacityList.isEmpty()) {
            return null;
        }
        return tenantCapacityList.get(0);
    }

    public boolean insertTenantCapacity(final TenantCapacity tenantCapacity) {
        ConfigInfo configInfoParam=new ConfigInfo();
        configInfoParam.setTenantId(tenantCapacity.getTenantId());
        int configCount = configInfoService.count(new QueryWrapper<>(configInfoParam));
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantCapacity.getTenantId());
        tenantCapacityParam.setQuota(tenantCapacity.getQuota());
        tenantCapacityParam.setMaxSize(tenantCapacity.getMaxSize());
        tenantCapacityParam.setMaxAggrCount(tenantCapacity.getMaxAggrCount());
        tenantCapacityParam.setMaxAggrSize(tenantCapacity.getMaxAggrSize());
        tenantCapacityParam.setGmtCreate(tenantCapacity.getGmtCreate());
        tenantCapacityParam.setGmtModified(tenantCapacity.getGmtModified());
        tenantCapacityParam.setUsage(configCount);
        return tenantCapacityService.save(tenantCapacityParam);

    }

    public boolean incrementUsageWithDefaultQuotaLimit(TenantCapacity tenantCapacity) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantCapacity.getTenantId());
        tenantCapacityParam.setQuota(0);
        TenantCapacity tenantCapacityR = tenantCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).lt(TenantCapacity.USAGE,tenantCapacity.getUsage()));
        tenantCapacityR.setGmtModified(tenantCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return tenantCapacityService.updateById(tenantCapacityR);
    }

    public boolean incrementUsageWithQuotaLimit(TenantCapacity tenantCapacity) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantCapacity.getTenantId());
        TenantCapacity tenantCapacityR = tenantCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).ne(TenantCapacity.QUOTA,0));
        tenantCapacityR.setGmtModified(tenantCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return tenantCapacityService.updateById(tenantCapacityR);
    }

    public boolean incrementUsage(TenantCapacity tenantCapacity) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantCapacity.getTenantId());
        TenantCapacity tenantCapacityR = tenantCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam));
        tenantCapacityR.setGmtModified(tenantCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return tenantCapacityService.updateById(tenantCapacityR);
    }

    public boolean decrementUsage(TenantCapacity tenantCapacity) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setTenantId(tenantCapacity.getTenantId());
        TenantCapacity tenantCapacityR = tenantCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).gt(TenantCapacity.USAGE,0));
        tenantCapacityR.setGmtModified(tenantCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()-1);
        return tenantCapacityService.updateById(tenantCapacityR);
    }

    public boolean updateTenantCapacity(String tenant, Integer quota, Integer maxSize, Integer maxAggrCount,
                                        Integer maxAggrSize) {
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        if (quota != null) {
            tenantCapacityParam.setQuota(quota);
        }
        if (maxSize != null) {
            tenantCapacityParam.setMaxSize(maxSize);
        }
        if (maxAggrCount != null) {
            tenantCapacityParam.setMaxAggrCount(maxAggrCount);
        }
        if (maxAggrSize != null) {
            tenantCapacityParam.setMaxAggrSize(maxAggrSize);
        }
        tenantCapacityParam.setGmtModified(TimeUtils.getCurrentTime());
        TenantCapacity tenantCapacity=new TenantCapacity();
        tenantCapacity.setTenantId(tenant);
        return tenantCapacityService.update(tenantCapacityParam,new QueryWrapper<>(tenantCapacity));
    }

    public boolean updateQuota(String tenant, Integer quota) {
        return updateTenantCapacity(tenant, quota, null, null, null);
    }

    public boolean correctUsage(String tenant, Timestamp gmtModified) {
        ConfigInfo configInfoParam=new ConfigInfo();
        configInfoParam.setTenantId(tenant);
        int configCount = configInfoService.count(new QueryWrapper<>(configInfoParam));
        TenantCapacity tenantCapacityParam=new TenantCapacity();
        tenantCapacityParam.setGmtModified(gmtModified);
        tenantCapacityParam.setUsage(configCount);
        TenantCapacity tenantCapacity=new TenantCapacity();
        tenantCapacity.setTenantId(tenant);
        return tenantCapacityService.update(tenantCapacityParam,new QueryWrapper<>(tenantCapacity));
    }

    /**
     * 获取TenantCapacity列表，只有id、tenantId有值
     *
     * @param lastId   id > lastId
     * @param pageSize 页数
     * @return TenantCapacity列表
     */
    public List<TenantCapacity> getCapacityList4CorrectUsage(long lastId, int pageSize) {
        List<TenantCapacity> tenantCapacityList = tenantCapacityService.list(new QueryWrapper<TenantCapacity>().gt(TenantCapacity.ID,lastId));
        if (tenantCapacityList!=null && !tenantCapacityList.isEmpty()){
            return tenantCapacityList.subList(0,pageSize-1);
        }
        return null;
    }

    public boolean deleteTenantCapacity(final String tenant) {
        TenantCapacity tenantCapacity=new TenantCapacity();
        tenantCapacity.setTenantId(tenant);
        return tenantCapacityService.remove(new QueryWrapper<>(tenantCapacity));
    }
}
