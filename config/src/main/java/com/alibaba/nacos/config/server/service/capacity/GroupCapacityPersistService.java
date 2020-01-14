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

import com.alibaba.nacos.config.server.mybatis.domain.entity.Capacity;
import com.alibaba.nacos.config.server.mybatis.domain.entity.ConfigInfo;
import com.alibaba.nacos.config.server.mybatis.domain.entity.GroupCapacity;
import com.alibaba.nacos.config.server.mybatis.domain.entity.TenantCapacity;
import com.alibaba.nacos.config.server.mybatis.service.ConfigInfoService;
import com.alibaba.nacos.config.server.mybatis.service.GroupCapacityService;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.List;


/**
 * Group Capacity Service
 *
 * @author hexu.hxy
 * @date 2018/03/05
 */
@Service
public class GroupCapacityPersistService {
    static final String CLUSTER = "";

    @Autowired
    private GroupCapacityService groupCapacityService;
    @Autowired
    private ConfigInfoService configInfoService;


    public GroupCapacity getGroupCapacity(String groupId) {
        GroupCapacity groupCapacity=new GroupCapacity();
        groupCapacity.setGroupId(groupId);
        List<GroupCapacity> list =groupCapacityService.list(new QueryWrapper<>(groupCapacity));
        if (list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    public Capacity getClusterCapacity() {
        return getGroupCapacity(CLUSTER);
    }

    public boolean insertGroupCapacity(final GroupCapacity capacity) {
        ConfigInfo configInfoParam=new ConfigInfo();
        if (!CLUSTER.equals(capacity.getGroupId())) {
            configInfoParam.setGroupId(capacity.getGroupId());
            configInfoParam.setTenantId("");
        }
        int configCount = configInfoService.count(new QueryWrapper<>(configInfoParam));

        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGroupId(capacity.getGroupId());
        tenantCapacityParam.setQuota(capacity.getQuota());
        tenantCapacityParam.setMaxSize(capacity.getMaxSize());
        tenantCapacityParam.setMaxAggrCount(capacity.getMaxAggrCount());
        tenantCapacityParam.setMaxAggrSize(capacity.getMaxAggrSize());
        tenantCapacityParam.setGmtCreate(capacity.getGmtCreate());
        tenantCapacityParam.setGmtModified(capacity.getGmtModified());
        tenantCapacityParam.setUsage(configCount);
        return groupCapacityService.save(tenantCapacityParam);
    }

    public int getClusterUsage() {
        Capacity clusterCapacity = getClusterCapacity();
        if (clusterCapacity != null) {
            return clusterCapacity.getUsage();
        }
        Integer result = configInfoService.count();
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result.intValue();
    }

    public boolean incrementUsageWithDefaultQuotaLimit(GroupCapacity groupCapacity) {
        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGroupId(groupCapacity.getGroupId());
        tenantCapacityParam.setQuota(0);
        GroupCapacity tenantCapacityR = groupCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).lt(TenantCapacity.USAGE,groupCapacity.getUsage()));
        tenantCapacityR.setGmtModified(groupCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return groupCapacityService.updateById(tenantCapacityR);
    }

    public boolean incrementUsageWithQuotaLimit(GroupCapacity groupCapacity) {
        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGroupId(groupCapacity.getGroupId());
        GroupCapacity tenantCapacityR = groupCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).ne(TenantCapacity.QUOTA,0));
        tenantCapacityR.setGmtModified(groupCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return groupCapacityService.updateById(tenantCapacityR);
    }

    public boolean incrementUsage(GroupCapacity groupCapacity) {
        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGroupId(groupCapacity.getGroupId());
        GroupCapacity tenantCapacityR = groupCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam));
        tenantCapacityR.setGmtModified(groupCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()+1);
        return groupCapacityService.updateById(tenantCapacityR);
    }

    public boolean decrementUsage(GroupCapacity groupCapacity) {
        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGroupId(groupCapacity.getGroupId());
        GroupCapacity tenantCapacityR = groupCapacityService.getOne(new QueryWrapper<>(tenantCapacityParam).gt(TenantCapacity.USAGE,0));
        tenantCapacityR.setGmtModified(groupCapacity.getGmtModified());
        tenantCapacityR.setUsage(tenantCapacityR.getUsage()-1);
        return groupCapacityService.updateById(tenantCapacityR);
    }

    public boolean updateGroupCapacity(String group, Integer quota, Integer maxSize, Integer maxAggrCount,
                                       Integer maxAggrSize) {
        GroupCapacity tenantCapacityParam=new GroupCapacity();
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
        GroupCapacity groupCapacity=new GroupCapacity();
        groupCapacity.setGroupId(group);
        return groupCapacityService.update(tenantCapacityParam,new QueryWrapper<>(groupCapacity));
    }

    public boolean updateQuota(String group, Integer quota) {
        return updateGroupCapacity(group, quota, null, null, null);
    }

    public boolean updateMaxSize(String group, Integer maxSize) {
        return updateGroupCapacity(group, null, maxSize, null, null);
    }

    public boolean correctUsage(String group, Timestamp gmtModified) {
        ConfigInfo configInfoParam=new ConfigInfo();
        if (!CLUSTER.equals(group)) {
            configInfoParam.setGroupId(group);
            configInfoParam.setTenantId("");
        }
        int configCount = configInfoService.count(new QueryWrapper<>(configInfoParam));
        GroupCapacity tenantCapacityParam=new GroupCapacity();
        tenantCapacityParam.setGmtModified(gmtModified);
        tenantCapacityParam.setUsage(configCount);
        GroupCapacity groupCapacity=new GroupCapacity();
        groupCapacity.setGroupId(group);
        return groupCapacityService.update(tenantCapacityParam,new QueryWrapper<>(groupCapacity));
    }

    /**
     * 获取GroupCapacity列表，只有id、groupId有值
     *
     * @param lastId   id > lastId
     * @param pageSize 页数
     * @return GroupCapacity列表
     */
    public List<GroupCapacity> getCapacityList4CorrectUsage(long lastId, int pageSize) {
        List<GroupCapacity> tenantCapacityList = groupCapacityService.list(new QueryWrapper<GroupCapacity>().gt(GroupCapacity.ID,lastId));
        if (tenantCapacityList!=null && !tenantCapacityList.isEmpty()){
            return tenantCapacityList.subList(0,pageSize-1);
        }
        return null;
    }

    public boolean deleteGroupCapacity(final String group) {
        GroupCapacity groupCapacity=new GroupCapacity();
        groupCapacity.setGroupId(group);
        return groupCapacityService.remove(new QueryWrapper<>(groupCapacity));
    }
}
