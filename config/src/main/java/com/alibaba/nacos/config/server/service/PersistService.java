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
package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.exception.NacosException;
import com.alibaba.nacos.config.server.model.*;
import com.alibaba.nacos.config.server.mybatis.domain.entity.ConfigInfo;
import com.alibaba.nacos.config.server.mybatis.domain.entity.ConfigInfoAggr;
import com.alibaba.nacos.config.server.mybatis.domain.entity.TenantInfo;
import com.alibaba.nacos.config.server.mybatis.domain.entity.*;
import com.alibaba.nacos.config.server.mybatis.service.*;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5;
import com.alibaba.nacos.config.server.utils.PaginationHelper;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;


import static com.alibaba.nacos.config.server.utils.LogUtil.defaultLog;
import static com.alibaba.nacos.config.server.utils.LogUtil.fatalLog;

/**
 * 数据库服务，提供ConfigInfo在数据库的存取<br> 3.0开始增加数据版本号, 并将物理删除改为逻辑删除<br> 3.0增加数据库切换功能
 *
 * @author boyan
 * @author leiwen.zh
 * @since 1.0
 */

@Repository
public class PersistService {

    @Autowired
    private UsersService usersService;

    @Autowired
    private ConfigInfoService configInfoService;

    @Autowired
    private ConfigInfoTagService configInfoTagService;

    @Autowired
    private ConfigTagsRelationService configTagsRelationService;

    @Autowired
    private HisConfigInfoService hisConfigInfoService;


    @Autowired
    private TenantInfoService tenantInfoService;

    @Autowired
    private ConfigInfoBetaService configInfoBetaService;

    @Autowired
    private ConfigInfoAggrService configInfoAggrService;


    @Autowired
    private AppConfigdataRelationSubsService appConfigdataRelationSubsService;



    /**
     * @author klw
     * @Description: constant variables
     */
    public static final String SPOT = ".";



    // ----------------------- config_info 表 insert update delete

    /**
     * 添加普通配置信息，发布数据变更事件
     */
    public void addConfigInfo(final String srcIp, final String srcUser, final ConfigInfo configInfo,
                              final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        long configId = addConfigInfoAtomic(srcIp, srcUser, configInfo, time, configAdvanceInfo);
        String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("config_tags");
        addConfiTagsRelationAtomic(configId, configTags, configInfo.getDataId(), configInfo.getGroupId(),
                configInfo.getTenantId());
        insertConfigHistoryAtomic(configId, configInfo, srcIp, srcUser, time, "I");

        if (notify) {
            EventDispatcher.fireEvent(
                    new ConfigDataChangeEvent(false, configInfo.getDataId(), configInfo.getGroupId(),
                            configInfo.getTenantId(), time.getTime()));
        }

    }

    /**
     * 添加普通配置信息，发布数据变更事件
     */
    public void addConfigInfo4Beta(ConfigInfo configInfo, String betaIps,
                                   String srcIp, String srcUser, Timestamp time, boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        try {
            String md5 = MD5.getInstance().getMD5String(configInfo.getContent());
            ConfigInfoBeta configInfoBeta=new ConfigInfoBeta();
            configInfoBeta.setDataId(configInfo.getDataId());
            configInfoBeta.setGroupId(configInfo.getGroupId());
            configInfoBeta.setTenantId(tenantTmp);
            configInfoBeta.setAppName(appNameTmp);
            configInfoBeta.setContent(configInfo.getContent());
            configInfoBeta.setMd5(md5);
            configInfoBeta.setBetaIps(betaIps);
            configInfoBeta.setSrcIp(srcIp);
            configInfoBeta.setSrcUser(srcUser);
            configInfoBeta.setGmtCreate(time);
            configInfoBeta.setGmtModified(time);
            configInfoBetaService.save(configInfoBeta);
            if (notify) {
                EventDispatcher.fireEvent(new ConfigDataChangeEvent(true, configInfo.getDataId(), configInfo.getGroupId(),
                        tenantTmp, time.getTime()));
            }
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 添加普通配置信息，发布数据变更事件
     */
    public void addConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
                                  boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        String tagTmp = StringUtils.isBlank(tag) ? null : tag.trim();
        try {
            String md5 = MD5.getInstance().getMD5String(configInfo.getContent());
            ConfigInfoTag configInfoTag=new ConfigInfoTag();
            configInfoTag.setDataId(configInfo.getDataId());
            configInfoTag.setGroupId(configInfo.getGroupId());
            configInfoTag.setTenantId(tenantTmp);
            configInfoTag.setAppName(appNameTmp);
            configInfoTag.setContent(configInfo.getContent());
            configInfoTag.setMd5(md5);
            configInfoTag.setSrcIp(srcIp);
            configInfoTag.setSrcUser(srcUser);
            configInfoTag.setGmtCreate(time);
            configInfoTag.setGmtModified(time);
            configInfoTagService.save(configInfoTag);
            if (notify) {
                EventDispatcher.fireEvent(new ConfigDataChangeEvent(false, configInfo.getDataId(),
                        configInfo.getGroupId(), tenantTmp, tagTmp, time.getTime()));
            }
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 更新配置信息
     */
    public void updateConfigInfo(final ConfigInfo configInfo, final String srcIp, final String srcUser,
                                 final Timestamp time, final Map<String, Object> configAdvanceInfo,
                                 final boolean notify) {
        ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroupId(),
                configInfo.getTenantId());
        String appNameTmp = oldConfigInfo.getAppName();
        // 用户传过来的appName不为空，则用持久化用户的appName，否则用db的;清空appName的时候需要传空串
        if (configInfo.getAppName() == null) {
            configInfo.setAppName(appNameTmp);
        }
        updateConfigInfoAtomic(configInfo, srcIp, srcUser, time, configAdvanceInfo);
        String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");
        if (configTags != null) {
            // 删除所有tag，然后再重新创建
            removeTagByIdAtomic(oldConfigInfo.getId());
            addConfiTagsRelationAtomic(oldConfigInfo.getId(), configTags, configInfo.getDataId(),
                    configInfo.getGroupId(), configInfo.getTenantId());
        }
        insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp, srcUser, time, "U");
        if (notify) {
            EventDispatcher.fireEvent(new ConfigDataChangeEvent(false, configInfo.getDataId(),
                    configInfo.getGroupId(), configInfo.getTenantId(), time.getTime()));
        }
    }

    /**
     * 更新配置信息
     */
    public void updateConfigInfo4Beta(ConfigInfo configInfo, String srcIp, String srcUser, Timestamp time,
                                      boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ?  null: configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        try {
            String md5 = MD5.getInstance().getMD5String(configInfo.getContent());
            ConfigInfoBeta configInfoBeta=new ConfigInfoBeta();
            configInfoBeta.setContent(configInfo.getContent());
            configInfoBeta.setMd5(md5);
            configInfoBeta.setSrcIp(srcIp);
            configInfoBeta.setSrcUser(srcUser);
            configInfoBeta.setGmtModified(time);
            configInfoBeta.setAppName(appNameTmp);
            ConfigInfoBeta configInfoBetaQuery=new ConfigInfoBeta();
            configInfoBetaQuery.setDataId(configInfo.getDataId());
            configInfoBetaQuery.setGroupId(configInfo.getGroupId());
            configInfoBetaService.update(configInfoBeta,new QueryWrapper<>(configInfoBetaQuery).eq(ConfigInfoTag.TENANT_ID,tenantTmp));
            if (notify) {
                EventDispatcher.fireEvent(new ConfigDataChangeEvent(true, configInfo.getDataId(), configInfo.getGroupId(),
                        tenantTmp, time.getTime()));
            }

        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 更新配置信息
     */
    public void updateConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
                                     boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        String tagTmp = StringUtils.isBlank(tag) ? null : tag.trim();
        try {
            String md5 = MD5.getInstance().getMD5String(configInfo.getContent());
            ConfigInfoTag configInfoTag=new ConfigInfoTag();
            configInfoTag.setContent(configInfo.getContent());
            configInfoTag.setMd5(md5);
            configInfoTag.setSrcIp(srcIp);
            configInfoTag.setSrcUser(srcUser);
            configInfoTag.setGmtModified(time);
            configInfoTag.setAppName(appNameTmp);
            ConfigInfoTag configInfoTagQuery=new ConfigInfoTag();
            configInfoTagQuery.setDataId(configInfo.getDataId());
            configInfoTagQuery.setGroupId(configInfo.getGroupId());
            configInfoTagQuery.setTagId(tagTmp);
            configInfoTagService.update(configInfoTag,new QueryWrapper<>(configInfoTagQuery).eq(ConfigInfoTag.TENANT_ID,tenantTmp));
            if (notify) {
                EventDispatcher.fireEvent(new ConfigDataChangeEvent(true, configInfo.getDataId(), configInfo.getGroupId(),
                        tenantTmp, tagTmp, time.getTime()));
            }

        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    public void insertOrUpdateBeta(final ConfigInfo configInfo, final String betaIps, final String srcIp,
                                   final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // 唯一性约束冲突
            updateConfigInfo4Beta(configInfo, srcIp, null, time, notify);
        }
    }

    public void insertOrUpdateTag(final ConfigInfo configInfo, final String tag, final String srcIp,
                                  final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // 唯一性约束冲突
            updateConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        }
    }

    /**
     * 更新md5
     */
    public void updateMd5(String dataId, String group, String tenant, String md5, Timestamp lastTime) {

        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        try {
            ConfigInfo configInfo=new ConfigInfo();
            configInfo.setMd5(md5);
            ConfigInfo configInfoQuery=new ConfigInfo();
            configInfoQuery.setDataId(dataId);
            configInfoQuery.setGroupId(group);
            configInfoQuery.setGmtModified(lastTime);
            configInfoService.update(configInfo,new QueryWrapper<>(configInfoQuery).eq(ConfigInfoTag.TENANT_ID,tenantTmp));
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }

    }

    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time,
                               Map<String, Object> configAdvanceInfo) {
        insertOrUpdate(srcIp, srcUser, configInfo, time, configAdvanceInfo, true);
    }

    /**
     * 写入主表，插入或更新
     */
    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time,
                               Map<String, Object> configAdvanceInfo, boolean notify) {
        try {
            addConfigInfo(srcIp, srcUser, configInfo, time, configAdvanceInfo, notify);
        } catch (DataIntegrityViolationException ive) { // 唯一性约束冲突
            updateConfigInfo(configInfo, srcIp, srcUser, time, configAdvanceInfo, notify);
        }
    }

    /**
     * 写入主表，插入或更新
     */
    public void insertOrUpdateSub(SubInfo subInfo) {
        try {
            addConfigSubAtomic(subInfo.getDataId(), subInfo.getGroup(), subInfo.getAppName(), subInfo.getDate());
        } catch (DataIntegrityViolationException ive) { // 唯一性约束冲突
            updateConfigSubAtomic(subInfo.getDataId(), subInfo.getGroup(), subInfo.getAppName(), subInfo.getDate());
        }
    }

    /**
     * 删除配置信息, 物理删除
     */
    public void removeConfigInfo(final String dataId, final String group, final String tenant, final String srcIp,
                                 final String srcUser) {
        ConfigInfo configInfo = findConfigInfo(dataId, group, tenant);
        final Timestamp time = new Timestamp(System.currentTimeMillis());
        if (configInfo != null) {
            removeConfigInfoAtomic(dataId, group, tenant, srcIp, srcUser);
            removeTagByIdAtomic(configInfo.getId());
            insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
        }
    }

    /**
     * @author klw
     * @Description: delete config info by ids
     * @Date 2019/7/5 16:45
     * @Param [ids, srcIp, srcUser]
     * @return List<ConfigInfo> deleted configInfos
     */
    @Transactional
    public List<ConfigInfo> removeConfigInfoByIds(final List<Long> ids, final String srcIp, final String srcUser) {
        if(CollectionUtils.isEmpty(ids)){
            return null;
        }
        ids.removeAll(Collections.singleton(null));
        try {
            final Timestamp time = new Timestamp(System.currentTimeMillis());
            Collection<ConfigInfo> configInfoList = findConfigInfosByIds(ids);
            if (!CollectionUtils.isEmpty(configInfoList)) {
                removeConfigInfoByIdsAtomic(ids);
                for(ConfigInfo configInfo : configInfoList){
                    removeTagByIdAtomic(configInfo.getId());
                    insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
                }
            }
            return Lists.newArrayList(configInfoList);
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
//        return tjt.execute(new TransactionCallback<List<ConfigInfo>>() {
//            final Timestamp time = new Timestamp(System.currentTimeMillis());
//
//            @Override
//            public List<ConfigInfo> doInTransaction(TransactionStatus status) {
//                try {
//                    String idsStr = Joiner.on(",").join(ids);
//                    List<ConfigInfo> configInfoList = findConfigInfosByIds(idsStr);
//                    if (!CollectionUtils.isEmpty(configInfoList)) {
//                        removeConfigInfoByIdsAtomic(idsStr);
//                        for(ConfigInfo configInfo : configInfoList){
//                            removeTagByIdAtomic(configInfo.getId());
//                            insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
//                        }
//                    }
//                    return configInfoList;
//                } catch (CannotGetJdbcConnectionException e) {
//                    fatalLog.error("[db-error] " + e.toString(), e);
//                    throw e;
//                }
//            }
//        });
    }

    /**
     * 删除beta配置信息, 物理删除
     */
    public void removeConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfo4Beta configInfo = findConfigInfo4Beta(dataId, group, tenant);
        if (configInfo != null) {
            ConfigInfoBeta configInfoBeta=new ConfigInfoBeta();
            configInfoBeta.setDataId(dataId);
            configInfoBeta.setGroupId(group);
            configInfoBetaService.remove(new QueryWrapper<>(configInfoBeta).eq(ConfigInfoTag.TENANT_ID,tenantTmp));
        }
    }

    // ----------------------- config_aggr_info 表 insert update delete

    /**
     * 增加聚合前数据到数据库, select -> update or insert
     */
    public boolean addAggrConfigInfo(final String dataId, final String group, String tenant, final String datumId,
                                     String appName, final String content) {
        String appNameTmp = StringUtils.isBlank(appName) ? null : appName;
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setDatumId(datumId);
        configInfoAggr.setTenantId(tenantTmp);
        ConfigInfoAggr configInfoAggr1= configInfoAggrService.getOne(new QueryWrapper<>(configInfoAggr));
        if(configInfoAggr1==null){
            ConfigInfoAggr configInfoAggrCon=new ConfigInfoAggr();
            configInfoAggrCon.setDataId(dataId);
            configInfoAggrCon.setGroupId(group);
            configInfoAggrCon.setTenantId(tenantTmp);
            configInfoAggrCon.setDatumId(datumId);
            configInfoAggrCon.setAppName(appNameTmp);
            configInfoAggrCon.setContent(content);
            configInfoAggrCon.setGmtModified(now);
            return configInfoAggrService.save(configInfoAggrCon);
        }else {
            String dbContent =configInfoAggr1.getContent();
            if (dbContent != null && dbContent.equals(content)) {
                return true;
            } else {
                ConfigInfoAggr configInfoAggrCon=new ConfigInfoAggr();
                configInfoAggrCon.setContent(dbContent);
                configInfoAggrCon.setGmtModified(now);
                return configInfoAggrService.update(configInfoAggrCon,new QueryWrapper<>(configInfoAggr));
            }
        }
    }

    /**
     * 删除单条聚合前数据
     */
    public void removeSingleAggrConfigInfo(final String dataId,
                                           final String group, final String tenant, final String datumId) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDatumId(datumId);
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        configInfoAggrService.remove(new QueryWrapper<>(configInfoAggr));
    }

    /**
     * 删除一个dataId下面所有的聚合前数据
     */
    public void removeAggrConfigInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        configInfoAggrService.remove(new QueryWrapper<>(configInfoAggr));
    }

    /**
     * 批量删除聚合数据，需要指定datum的列表
     *
     * @param dataId
     * @param group
     * @param datumList
     */
    public boolean batchRemoveAggr(final String dataId, final String group, final String tenant,
                                   final List<String> datumList) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        return configInfoAggrService.remove(new QueryWrapper<>(configInfoAggr).in(ConfigInfoAggr.DATUM_ID,datumList));
    }

    /**
     * 删除startTime前的数据
     */
    public void removeConfigHistory(final Timestamp startTime, final int limitSize) {
        Page page=new Page();
        page.setSize(limitSize);
        page.setSearchCount(false);

        IPage<HisConfigInfo> hisConfigInfoPage= hisConfigInfoService.page(page,new QueryWrapper<HisConfigInfo>().le(HisConfigInfo.GMT_MODIFIED,startTime));
        List<Long> ids= hisConfigInfoPage.getRecords().stream().map(vo->vo.getId()).collect(Collectors.toList());
        hisConfigInfoService.removeByIds(ids);
    }

    /**
     * 获取指定时间前配置条数
     */
    public int findConfigHistoryCountByTime(final Timestamp startTime) {
        Integer result = hisConfigInfoService.count(new QueryWrapper<HisConfigInfo>().le(HisConfigInfo.GMT_MODIFIED,startTime));
        if (result == null) {
            throw new IllegalArgumentException("configInfoBetaCount error");
        }
        return result.intValue();
    }

    /**
     * 获取最大maxId
     */
    public long findConfigMaxId() {
        List<ConfigInfo> configInfoList=configInfoService.list(new QueryWrapper<ConfigInfo>().orderByDesc(ConfigInfo.ID));
        if(configInfoList!=null && !configInfoList.isEmpty()){
            return configInfoList.get(0).getId();
        }
        return 0L;
    }

    /**
     * 批量添加或者更新数据.事务过程中出现任何异常都会强制抛出TransactionSystemException
     *
     * @param dataId
     * @param group
     * @param datumMap
     * @return
     */
    public boolean batchPublishAggr(final String dataId, final String group, final String tenant,
                                    final Map<String, String> datumMap, final String appName) {
        for (Entry<String, String> entry : datumMap.entrySet()) {
            try {
                if (!addAggrConfigInfo(dataId, group, tenant, entry.getKey(), appName, entry.getValue())) {
                    throw new TransactionSystemException(
                            "error in addAggrConfigInfo");
                }
            } catch (Throwable e) {
                throw new TransactionSystemException(
                        "error in addAggrConfigInfo");
            }
        }
        return true;
    }

    /**
     * 批量替换，先全部删除聚合表中指定DataID+Group的数据，再插入数据. 事务过程中出现任何异常都会强制抛出TransactionSystemException
     *
     * @param dataId
     * @param group
     * @param datumMap
     * @return
     */
    public boolean replaceAggr(final String dataId, final String group, final String tenant,
                               final Map<String, String> datumMap, final String appName) {
        try {
            String appNameTmp = appName == null ? "" : appName;
            removeAggrConfigInfo(dataId, group, tenant);
            String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
            List<ConfigInfoAggr> configInfoAggrs=new ArrayList<>();
            for (Entry<String, String> datumEntry : datumMap.entrySet()) {
                ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
                configInfoAggr.setDataId(dataId);
                configInfoAggr.setGroupId(group);
                configInfoAggr.setTenantId(tenantTmp);
                configInfoAggr.setAppName(appNameTmp);
                configInfoAggr.setDatumId(datumEntry.getKey());
                configInfoAggr.setContent(datumEntry.getValue());
                configInfoAggr.setGmtModified(new Timestamp(System.currentTimeMillis()));
                configInfoAggrs.add(configInfoAggr);
            }
            configInfoAggrService.saveBatch(configInfoAggrs);
        } catch (Throwable e) {
            throw new TransactionSystemException(
                    "error in addAggrConfigInfo");
        }
        return true;
    }

    /**
     * 查找所有的dataId和group。保证不返回NULL。
     */
    @Deprecated
    public List<ConfigInfo> findAllDataIdAndGroup() {
        List<ConfigInfo> configInfos= configInfoService.list(new QueryWrapper<ConfigInfo>().select(new String[]{ConfigInfo.DATA_ID,ConfigInfo.GROUP_ID}));
        if(CollectionUtils.isEmpty(configInfos)){
            return Lists.newArrayList();
        }
        return configInfos.stream().distinct().collect(Collectors.toList());
    }

    /**
     * 根据dataId和group查询配置信息
     */
    public ConfigInfo4Beta findConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfoBeta configInfoBeta=new ConfigInfoBeta();
        configInfoBeta.setDataId(dataId);
        configInfoBeta.setGroupId(group);
        configInfoBeta.setTenantId(tenantTmp);
        ConfigInfoBeta configInfoBeta1 = configInfoBetaService.getOne(new QueryWrapper<>(configInfoBeta));
        if(configInfoBeta1==null){
            return null;
        }
        ConfigInfo4Beta configInfo4Beta=new ConfigInfo4Beta(configInfoBeta1);
        return configInfo4Beta;
    }

    /**
     * 根据dataId和group查询配置信息
     */
    public ConfigInfo4Tag findConfigInfo4Tag(final String dataId, final String group, final String tenant,
                                             final String tag) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        String tagTmp = StringUtils.isBlank(tag) ? null : tag.trim();
        ConfigInfoTag configInfoTag=new ConfigInfoTag();
        configInfoTag.setDataId(dataId);
        configInfoTag.setGroupId(group);
        configInfoTag.setTagId(tagTmp);
        configInfoTag.setTenantId(tenantTmp);
        ConfigInfoTag configInfoTag1=configInfoTagService.getOne(new QueryWrapper<>(configInfoTag));
        if(configInfoTag1==null){
            return null;
        }
        ConfigInfo4Tag configInfo4Tag=new ConfigInfo4Tag(configInfoTag1);
        return configInfo4Tag;
    }

    /**
     * 根据dataId和group查询配置信息
     */
    public ConfigInfo findConfigInfoApp(final String dataId, final String group, final String tenant,
                                        final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        configInfo.setAppName(appName);
        configInfo.setTenantId(tenantTmp);
        ConfigInfo configInfo1= configInfoService.getOne(new QueryWrapper<>(configInfo));
        return configInfo1;
    }

    /**
     * 根据dataId和group查询配置信息
     */
    public ConfigInfo findConfigInfoAdvanceInfo(final String dataId, final String group, final String tenant,
                                                final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");

        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        configInfo.setTenantId(tenantTmp);

        if (StringUtils.isNotBlank(appName)) {
            configInfo.setAppName(appName);
        }
        ConfigInfo configInfo1;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());

                configInfo1= configInfoService.getOne(new QueryWrapper<>(configInfo).in(ConfigInfo.ID,ids));
                return configInfo1;
            }

        }
        configInfo1= configInfoService.getOne(new QueryWrapper<>(configInfo));
        return configInfo1;
    }

    /**
     * 根据dataId和group查询配置信息
     */
    public ConfigInfoBase findConfigInfoBase(final String dataId, final String group) {
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        ConfigInfo configInfo1= configInfoService.getOne(new QueryWrapper<>(configInfo));
        if(configInfo1==null){
            return null;
        }
        ConfigInfoBase configInfoBase=new ConfigInfoBase(configInfo1);
        return configInfoBase;
    }

    /**
     * 根据数据库主键ID查询配置信息
     *
     * @param id
     * @return
     */
    public ConfigInfo findConfigInfo(long id) {
        return configInfoService.getById(id);
    }

    /**
     * 根据dataId查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param dataId
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoByDataId(final int pageNo, final int pageSize, final String dataId,
                                                    final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    /**
     * 根据dataId查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param dataId
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoByDataIdAndApp(final int pageNo, final int pageSize, final String dataId,
                                                          final String tenant, final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setAppName(appName);
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    public IPage<ConfigInfo> findConfigInfoByDataIdAndAdvance(final int pageNo, final int pageSize, final String dataId,
                                                              final String tenant,
                                                              final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setTenantId(tenantTmp);

        if (StringUtils.isNotBlank(appName)) {
            configInfo.setAppName(appName);
        }
        IPage<ConfigInfo> configInfoIPage;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());

                configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo).in(ConfigInfo.ID,ids));
                return configInfoIPage;
            }

        }
        configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    public IPage<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId,
                                                 final String group,
                                                 final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");


        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        if (StringUtils.isNotBlank(appName)) {
            configInfo.setAppName(appName);
        }
        if (StringUtils.isNotBlank(dataId)) {
            configInfo.setDataId(dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            configInfo.setGroupId(group);
        }
        IPage<ConfigInfo> configInfoIPage;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());
                configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo).in(ConfigInfo.ID,ids));
                return configInfoIPage;
            }
        }
        configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    /**
     * 根据dataId查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param dataId
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfoBase> findConfigInfoBaseByDataId(final int pageNo,
                                                            final int pageSize, final String dataId) {

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo).eq(ConfigInfoTag.TENANT_ID,null));

        IPage<ConfigInfoBase> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoBase configInfoBase=new ConfigInfoBase(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 根据group查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param group
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoByGroup(final int pageNo, final int pageSize, final String group,
                                                   final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));

        return configInfoIPage;
    }

    /**
     * 根据group查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param group
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoByGroupAndApp(final int pageNo,
                                                         final int pageSize, final String group, final String tenant,
                                                         final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setGroupId(group);
        configInfo.setAppName(appName);
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));

        return configInfoIPage;
    }

    public IPage<ConfigInfo> findConfigInfoByGroupAndAdvance(final int pageNo,
                                                             final int pageSize, final String group, final String tenant,
                                                             final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();

        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        if (StringUtils.isNotBlank(appName)) {
            configInfo.setAppName(appName);
        }
        configInfo.setGroupId(group);
        IPage<ConfigInfo> configInfoIPage;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());

                configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo).in(ConfigInfo.ID,ids));
                return configInfoIPage;
            }

        }
        configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    /**
     * 根据group查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoByApp(final int pageNo,
                                                 final int pageSize, final String tenant, final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setAppName(appName);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo).like(ConfigInfo.TENANT_ID,tenantTmp));

        return configInfoIPage;
    }

    public IPage<ConfigInfo> findConfigInfoByAdvance(final int pageNo,
                                                     final int pageSize, final String tenant,
                                                     final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        if (StringUtils.isNotBlank(appName)) {
            configInfo.setAppName(appName);
        }
        IPage<ConfigInfo> configInfoIPage;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());

                configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo).in(ConfigInfo.ID,ids));
                return configInfoIPage;
            }

        }
        configInfoIPage= configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    /**
     * 根据group查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param group
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfoBase> findConfigInfoBaseByGroup(final int pageNo,
                                                           final int pageSize, final String group) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setGroupId(group);
        configInfo.setAppName(null);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));

        IPage<ConfigInfoBase> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoBase configInfoBase=new ConfigInfoBase(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 返回配置项个数
     */
    public int configInfoCount() {
        Integer result = configInfoService.count();
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result.intValue();
    }

    /**
     * 返回配置项个数
     */
    public int configInfoCount(String tenant) {
        Integer result = configInfoService.count(new QueryWrapper<ConfigInfo>().eq(ConfigInfo.TENANT_ID,tenant));
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result.intValue();
    }

    /**
     * 返回beta配置项个数
     */
    public int configInfoBetaCount() {
        Integer result = configInfoBetaService.count();
        if (result == null) {
            throw new IllegalArgumentException("configInfoBetaCount error");
        }
        return result.intValue();
    }

    /**
     * 返回beta配置项个数
     */
    public int configInfoTagCount() {
        Integer result = configInfoTagService.count();
        if (result == null) {
            throw new IllegalArgumentException("configInfoBetaCount error");
        }
        return result.intValue();
    }

    public List<String> getTenantIdList(int pageNo, int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<ConfigInfo>().notIn(ConfigInfo.TENANT_ID,Lists.newArrayList(new String[]{""})));
        return configInfoIPage.getRecords().stream().map(vo->vo.getTenantId()).collect(Collectors.toList());
    }

    public List<String> getGroupIdList(int pageNo, int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<ConfigInfo>().notIn(ConfigInfo.TENANT_ID,Lists.newArrayList(new String[]{""})));
        return configInfoIPage.getRecords().stream().map(vo->vo.getGroupId()).collect(Collectors.toList());
    }

    public int aggrConfigInfoCount(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        Integer result =configInfoAggrService.count(new QueryWrapper<>(configInfoAggr));
        if (result == null) {
            throw new IllegalArgumentException("aggrConfigInfoCount error");
        }
        return result.intValue();
    }

    public int aggrConfigInfoCountIn(String dataId, String group, String tenant, List<String> datumIds) {
        return aggrConfigInfoCount(dataId, group, tenant, datumIds, true);
    }

    public int aggrConfigInfoCountNotIn(String dataId, String group, String tenant, List<String> datumIds) {
        return aggrConfigInfoCount(dataId, group, tenant, datumIds, false);
    }

    private int aggrConfigInfoCount(String dataId, String group, String tenant, List<String> datumIds,
                                    boolean isIn) {
        if (datumIds == null || datumIds.isEmpty()) {
            return 0;
        }
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        ConfigTagsRelation configTagsRelation=new ConfigTagsRelation();
        configTagsRelation.setTenantId(tenantTmp);

        if (!CollectionUtils.isEmpty(datumIds)) {
            int configTagsRelations;
            if(datumIds.size()>0){
                if (isIn) {
                    configTagsRelations = configTagsRelationService.count(new QueryWrapper<>(configTagsRelation).in(ConfigTagsRelation.TAG_NAME,datumIds));
                } else {
                    configTagsRelations = configTagsRelationService.count(new QueryWrapper<>(configTagsRelation).notIn(ConfigTagsRelation.TAG_NAME,datumIds));
                }

                return configTagsRelations;
            }
        }
        return 0;
    }

    /**
     * 分页查询所有的配置信息
     *
     * @param pageNo   页码(从1开始)
     * @param pageSize 每页大小(必须大于0)
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findAllConfigInfo(final int pageNo, final int pageSize, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));
        return configInfoIPage;
    }

    /**
     * 分页查询所有的配置信息
     *
     * @param pageNo   页码(从1开始)
     * @param pageSize 每页大小(必须大于0)
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigKey> findAllConfigKey(final int pageNo, final int pageSize, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setTenantId(tenantTmp);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<>(configInfo));

        IPage<ConfigKey> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigKey configInfoBase=new ConfigKey(vo.getAppName(),vo.getDataId(),vo.getGroupId());return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 分页查询所有的配置信息
     *
     * @param pageNo   页码(从1开始)
     * @param pageSize 每页大小(必须大于0)
     * @return ConfigInfo对象的集合
     */
    @Deprecated
    public IPage<ConfigInfoBase> findAllConfigInfoBase(final int pageNo,
                                                       final int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page);

        IPage<ConfigInfoBase> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoBase configInfoBase=new ConfigInfoBase(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    public static class ConfigInfoWrapper extends ConfigInfo {
        private static final long serialVersionUID = 4511997359365712505L;

        private long lastModified;

        public ConfigInfoWrapper(ConfigInfo configInfo){
            this.setId(configInfo.getId());
            this.setGroupId(configInfo.getGroupId());
            this.setAppName(configInfo.getAppName());
            this.setTenantId(configInfo.getTenantId());
            this.setContent(configInfo.getContent());
            this.setDataId(configInfo.getDataId());
            this.setMd5(configInfo.getMd5());
            this.setLastModified(configInfo.getGmtModified().getTime());
        }

        public ConfigInfoWrapper() {
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    public static class ConfigInfoBetaWrapper extends ConfigInfo4Beta {
        private static final long serialVersionUID = 4511997359365712505L;

        private long lastModified;

        public ConfigInfoBetaWrapper() {
        }

        public ConfigInfoBetaWrapper(ConfigInfoBeta configInfo4Beta) {
            this.setId(configInfo4Beta.getId());
            this.setGroupId(configInfo4Beta.getGroupId());
            this.setAppName(configInfo4Beta.getAppName());
            this.setTenant(configInfo4Beta.getTenantId());
            this.setContent(configInfo4Beta.getContent());
            this.setDataId(configInfo4Beta.getDataId());
            this.setMd5(configInfo4Beta.getMd5());
            this.setBetaIps(configInfo4Beta.getBetaIps());
            this.setLastModified(configInfo4Beta.getGmtModified().getTime());

        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    public static class ConfigInfoTagWrapper extends ConfigInfo4Tag {
        private static final long serialVersionUID = 4511997359365712505L;

        private long lastModified;

        public ConfigInfoTagWrapper() {
        }

        public ConfigInfoTagWrapper(ConfigInfoTag configInfoTag) {
            this.setId(configInfoTag.getId());
            this.setGroupId(configInfoTag.getGroupId());
            this.setAppName(configInfoTag.getAppName());
            this.setTenant(configInfoTag.getTenantId());
            this.setContent(configInfoTag.getContent());
            this.setDataId(configInfoTag.getDataId());
            this.setMd5(configInfoTag.getMd5());
            this.setTag(configInfoTag.getTagId());
            this.setLastModified(configInfoTag.getGmtModified().getTime());
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    public IPage<ConfigInfoWrapper> findAllConfigInfoForDumpAll(
            final int pageNo, final int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page);

        IPage<ConfigInfoWrapper> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoWrapper configInfoBase=new ConfigInfoWrapper(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;

    }

    public IPage<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize) {
        Page page=new Page();
        page.setCurrent(0);
        page.setSize(pageSize);
        IPage<ConfigInfo> configInfoIPage = configInfoService.page(page,new QueryWrapper<ConfigInfo>().ge(ConfigInfo.ID,lastMaxId));

        IPage<ConfigInfoWrapper> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoWrapper configInfoBase=new ConfigInfoWrapper(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    public IPage<ConfigInfoBetaWrapper> findAllConfigInfoBetaForDumpAll(
            final int pageNo, final int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfoBeta> configInfoIPage = configInfoBetaService.page(page);

        IPage<ConfigInfoBetaWrapper> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoBetaWrapper configInfoBase=new ConfigInfoBetaWrapper(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    public IPage<ConfigInfoTagWrapper> findAllConfigInfoTagForDumpAll(
            final int pageNo, final int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        IPage<ConfigInfoTag> configInfoIPage = configInfoTagService.page(page);

        IPage<ConfigInfoTagWrapper> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoTagWrapper configInfoBase=new ConfigInfoTagWrapper(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 通过select in方式实现db记录的批量查询； subQueryLimit指定in中条件的个数，上限20
     */
    public List<ConfigInfo> findConfigInfoByBatch(final List<String> dataIds,
                                                  final String group, final String tenant, int subQueryLimit) {
        // assert dataIDs group not null
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        // if dataIDs empty return empty list
        if (CollectionUtils.isEmpty(dataIds)) {
            return Collections.emptyList();
        }

        // 批量查询上限
        // in 个数控制在100内, sql语句长度越短越好
        if (subQueryLimit > QUERY_LIMIT_SIZE) {
            subQueryLimit = 50;
        }
        List<ConfigInfo> result = new ArrayList<ConfigInfo>(dataIds.size());



        String sqlStart
                = "select DATA_ID, GROUP_ID, TENANT_ID, APP_NAME, CONTENT from CONFIG_INFO where GROUP_ID = ? and "
                + "TENANT_ID = ? and DATA_ID in (";
        String sqlEnd = ")";
        StringBuilder subQuerySql = new StringBuilder();

        for (int i = 0; i < dataIds.size(); i += subQueryLimit) {
            // dataIDs
            List<String> params = new ArrayList<String>(dataIds.subList(i, i
                    + subQueryLimit < dataIds.size() ? i + subQueryLimit
                    : dataIds.size()));

            for (int j = 0; j < params.size(); j++) {
                subQuerySql.append("?");
                if (j != params.size() - 1) {
                    subQuerySql.append(",");
                }
            }
            ConfigInfo configInfo=new ConfigInfo();
            configInfo.setGroupId(group);
            configInfo.setTenantId(tenantTmp);
            List<ConfigInfo> configInfos=null;
            if (!CollectionUtils.isEmpty(params)) {
                configInfos = configInfoService.list(new QueryWrapper<>(configInfo).in(ConfigTagsRelation.DATA_ID,params));
            }
            // assert not null
            if (configInfos != null && configInfos.size() > 0) {
                result.addAll(configInfos);
            }
        }
        return result;
    }

    /**
     * 根据dataId和group模糊查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param dataId   支持模糊查询
     * @param group    支持模糊查询
     * @param tenant   支持模糊查询
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoLike(final int pageNo, final int pageSize, final String dataId,
                                                final String group, final String tenant, final String appName,
                                                final String content) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group)) {
            if (StringUtils.isBlank(appName)) {
                return this.findAllConfigInfo(pageNo, pageSize, tenantTmp);
            } else {
                return this.findConfigInfoByApp(pageNo, pageSize, tenantTmp, appName);
            }
        }

        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        QueryWrapper queryWrapper=new QueryWrapper<ConfigInfo>();
        if (!StringUtils.isBlank(dataId)) {
            queryWrapper.like(ConfigInfo.DATA_ID,dataId);
        }
        if (!StringUtils.isBlank(group)) {
            queryWrapper.like(ConfigInfo.GROUP_ID,group);
        }
        if (!StringUtils.isBlank(appName)) {
            queryWrapper.eq(ConfigInfo.APP_NAME,appName);
        }
        if (!StringUtils.isBlank(content)) {
            queryWrapper.like(ConfigInfo.CONTENT,content);
        }

        queryWrapper.like(ConfigInfo.TENANT_ID,tenantTmp);
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        IPage<ConfigInfo> configInfoIPage =configInfoService.page(page,queryWrapper);
        return configInfoIPage;
    }

    public IPage<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId,
                                                     final String group, final String tenant,
                                                     final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        final String appName = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONTENT");
        final String configTags = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("CONFIG_TAGS");

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        QueryWrapper<ConfigInfo> queryWrapper= new QueryWrapper<ConfigInfo>();

        if (!StringUtils.isBlank(dataId)) {
            queryWrapper.like(ConfigInfo.DATA_ID,dataId);
        }
        if (!StringUtils.isBlank(group)) {
            queryWrapper.like(ConfigInfo.GROUP_ID,group);
        }
        if (!StringUtils.isBlank(appName)) {
            queryWrapper.eq(ConfigInfo.APP_NAME,appName);
        }
        if (!StringUtils.isBlank(content)) {
            queryWrapper.eq(ConfigInfo.CONTENT,content);
        }

        if(tenantTmp ==null){
            queryWrapper.isNull(ConfigInfo.TENANT_ID);
        }else {
            queryWrapper.eq(ConfigInfo.TENANT_ID,tenantTmp);
        }

        IPage<ConfigInfo> configInfoIPage;
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            List<ConfigTagsRelation> configTagsRelations=null;
            if(tagArr.length>0){
                configTagsRelations = configTagsRelationService.list(new QueryWrapper<ConfigTagsRelation>().in(ConfigTagsRelation.TAG_NAME,Lists.newArrayList(tagArr)));
                List<Long> ids= configTagsRelations.stream().map(vo->vo.getId()).collect(Collectors.toList());

                configInfoIPage= configInfoService.page(page,queryWrapper.in(ConfigInfo.ID,ids));
                return configInfoIPage;
            }

        }
        configInfoIPage= configInfoService.page(page,queryWrapper);
        return configInfoIPage;
    }

    /**
     * 根据dataId和group模糊查询配置信息
     *
     * @param pageNo     页码(必须大于0)
     * @param pageSize   每页大小(必须大于0)
     * @param configKeys 查询配置列表
     * @param blacklist  是否黑名单
     * @return ConfigInfo对象的集合
     */
    public IPage<ConfigInfo> findConfigInfoLike(final int pageNo,
                                                final int pageSize, final ConfigKey[] configKeys,
                                                final boolean blacklist) {
        // 白名单，请同步条件为空，则没有符合条件的配置
        if (configKeys.length == 0 && blacklist == false) {
            IPage<ConfigInfo> page = new Page<ConfigInfo>();
            page.setTotal(0);
            return page;
        }
        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        List<String> params = new ArrayList<String>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        QueryWrapper queryWrapper= new QueryWrapper<ConfigInfo>();

        boolean isFirst = true;
        for (ConfigKey configInfo : configKeys) {
            String dataId = configInfo.getDataId();
            String group = configInfo.getGroup();
            String appName = configInfo.getAppName();

            if (StringUtils.isBlank(dataId)
                    && StringUtils.isBlank(group)
                    && StringUtils.isBlank(appName)) {
                break;
            }
            if (blacklist) {
                boolean isFirstSub = true;
                if(!StringUtils.isBlank(dataId)) {
                    queryWrapper.notLike(ConfigInfo.DATA_ID,dataId);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {

                    }

                    queryWrapper.notLike(ConfigInfo.GROUP_ID,group);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {

                    }
                    queryWrapper.notLike(ConfigInfo.APP_NAME,appName);
                    isFirstSub = false;
                }
            }else {

                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    queryWrapper.notLike(ConfigInfo.DATA_ID,dataId);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {

                    }

                    queryWrapper.notLike(ConfigInfo.GROUP_ID,group);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {

                    }
                    queryWrapper.notLike(ConfigInfo.APP_NAME,appName);
                    isFirstSub = false;
                }
            }
        }
        return configInfoService.page(page,queryWrapper);
    }

    /**
     * 根据dataId和group模糊查询配置信息
     *
     * @param pageNo   页码(必须大于0)
     * @param pageSize 每页大小(必须大于0)
     * @param dataId
     * @param group
     * @return ConfigInfo对象的集合
     * @throws IOException
     */
    public IPage<ConfigInfoBase> findConfigInfoBaseLike(final int pageNo,
                                                        final int pageSize, final String dataId, final String group,
                                                        final String content) throws IOException {
        if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group)) {
            throw new IOException("invalid param");
        }

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        QueryWrapper queryWrapper= new QueryWrapper<ConfigInfo>();

        if (!StringUtils.isBlank(dataId)) {
            queryWrapper.like(ConfigInfo.DATA_ID,dataId);
        }
        if (!StringUtils.isBlank(group)) {
            queryWrapper.like(ConfigInfo.GROUP_ID,group);
        }
        if (!StringUtils.isBlank(content)) {
            queryWrapper.eq(ConfigInfo.CONTENT,content);
        }

        IPage<ConfigInfo> configInfoIPage;
        configInfoIPage= configInfoService.page(page,queryWrapper);

        IPage<ConfigInfoBase> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoBase configInfoBase=new ConfigInfoBase(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 查找聚合前的单条数据
     *
     * @param dataId
     * @param group
     * @param datumId
     * @return
     */
    public ConfigInfoAggr findSingleConfigInfoAggr(String dataId, String group, String tenant, String datumId) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        return configInfoAggrService.getOne(new QueryWrapper<>(configInfoAggr));
    }

    /**
     * 查找一个dataId下面的所有聚合前的数据. 保证不返回NULL.
     */
    public List<ConfigInfoAggr> findConfigInfoAggr(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(group);
        configInfoAggr.setTenantId(tenantTmp);
        List<ConfigInfoAggr> configInfoAggrs=configInfoAggrService.list(new QueryWrapper<>(configInfoAggr));
        return configInfoAggrs;
    }

    public IPage<ConfigInfoAggr> findConfigInfoAggrByPage(String dataId, String group, String tenant, final int pageNo,
                                                          final int pageSize) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        QueryWrapper queryWrapper= new QueryWrapper<ConfigInfoAggr>();

        if (!StringUtils.isBlank(dataId)) {
            queryWrapper.eq(ConfigInfo.DATA_ID,dataId);
        }
        if (!StringUtils.isBlank(group)) {
            queryWrapper.eq(ConfigInfo.GROUP_ID,group);
        }
        if(tenantTmp ==null){
            queryWrapper.isNull(ConfigInfo.TENANT_ID);
        }else {
            queryWrapper.eq(ConfigInfo.TENANT_ID,tenantTmp);
        }

        IPage<ConfigInfoAggr> configInfoIPage;

        configInfoIPage= configInfoService.page(page,queryWrapper);
        return configInfoIPage;
    }

    /**
     * 查询符合条件的聚合数据
     *
     * @param pageNo     pageNo
     * @param pageSize   pageSize
     * @param configKeys 聚合数据条件
     * @param blacklist  黑名单
     * @return
     */
    public IPage<ConfigInfoAggr> findConfigInfoAggrLike(final int pageNo, final int pageSize, ConfigKey[] configKeys,
                                                        boolean blacklist) {

        if (configKeys.length == 0 && blacklist == false) {
            IPage<ConfigInfoAggr> page = new Page<ConfigInfoAggr>();
            page.setTotal(0);
            return page;
        }

        PaginationHelper<ConfigInfo> helper = new PaginationHelper<ConfigInfo>();
        List<String> params = new ArrayList<String>();
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        QueryWrapper queryWrapper= new QueryWrapper<ConfigInfoAggr>();

        boolean isFirst = true;
        for (ConfigKey configInfo : configKeys) {
            String dataId = configInfo.getDataId();
            String group = configInfo.getGroup();
            String appName = configInfo.getAppName();

            if (StringUtils.isBlank(dataId)
                    && StringUtils.isBlank(group)
                    && StringUtils.isBlank(appName)) {
                break;
            }
            if (blacklist) {

                boolean isFirstSub = true;
                if(!StringUtils.isBlank(dataId)) {
                    queryWrapper.notLike(ConfigInfo.DATA_ID,dataId);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {

                    }

                    queryWrapper.notLike(ConfigInfo.GROUP_ID,group);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {

                    }
                    queryWrapper.notLike(ConfigInfo.APP_NAME,appName);
                    isFirstSub = false;
                }
            }else {

                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    queryWrapper.notLike(ConfigInfo.DATA_ID,dataId);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {

                    }

                    queryWrapper.notLike(ConfigInfo.GROUP_ID,group);
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {

                    }
                    queryWrapper.notLike(ConfigInfo.APP_NAME,appName);
                    isFirstSub = false;
                }
            }
        }
        return configInfoAggrService.page(page,queryWrapper);
    }

    /**
     * 找到所有聚合数据组。
     */
    public List<ConfigInfoChanged> findAllAggrGroup() {
        List<ConfigInfoAggr> list= configInfoAggrService.list(new QueryWrapper<ConfigInfoAggr>().select(new String[]{ConfigInfoAggr.DATA_ID,ConfigInfoAggr.GROUP_ID,ConfigInfoAggr.TENANT_ID}));

        Set<ConfigInfoAggr>  configInfoAggrs= Sets.newHashSet(list);
        return  configInfoAggrs.stream().map(vo->{ConfigInfoChanged configInfoChanged=new ConfigInfoChanged(vo.getDataId(),vo.getGroupId(),vo.getTenantId());return configInfoChanged;}).collect(Collectors.toList());

    }

    /**
     * 由datum内容查找datumId
     *
     * @param dataId  data id
     * @param groupId group
     * @param content content
     * @return datum keys
     */
    public List<String> findDatumIdByContent(String dataId, String groupId,
                                             String content) {
        ConfigInfoAggr configInfoAggr=new ConfigInfoAggr();
        configInfoAggr.setDataId(dataId);
        configInfoAggr.setGroupId(groupId);
        configInfoAggr.setContent(content);
        List<ConfigInfoAggr> list= configInfoAggrService.list(new QueryWrapper<>(configInfoAggr));
        return list.stream().map(vo->vo.getDatumId()).collect(Collectors.toList());
    }

    public List<ConfigInfoWrapper> findChangeConfig(final Timestamp startTime,
                                                    final Timestamp endTime) {
        List<Map<String, Object>> configInfos=configInfoService.listMaps(new QueryWrapper<ConfigInfo>().between(ConfigInfo.GMT_MODIFIED,startTime,endTime));
        return convertChangeConfig(configInfos);
    }

    /**
     * 根据时间段和配置条件查询符合条件的配置
     *
     * @param dataId    dataId 支持模糊
     * @param group     dataId 支持模糊
     * @param appName   产品名
     * @param startTime 起始时间
     * @param endTime   截止时间
     * @param pageNo    pageNo
     * @param pageSize  pageSize
     * @return
     */
    public IPage<ConfigInfoWrapper> findChangeConfig(final String dataId, final String group, final String tenant,
                                                     final String appName, final Timestamp startTime,
                                                     final Timestamp endTime, final int pageNo,
                                                     final int pageSize, final long lastMaxId) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        IPage page = new Page<>();
        page.setSize(pageSize);
        page.setCurrent(pageNo);
        QueryWrapper queryWrapper=new QueryWrapper<>();
        if (!StringUtils.isBlank(dataId)) {
            queryWrapper.like(ConfigInfo.DATA_ID,dataId);
        }
        if (!StringUtils.isBlank(group)) {
            queryWrapper.like(ConfigInfo.GROUP_ID,group);
        }

        if (!StringUtils.isBlank(tenantTmp)) {
            queryWrapper.like(ConfigInfo.TENANT_ID,tenantTmp);
        }

        if (!StringUtils.isBlank(appName)) {
            queryWrapper.eq(ConfigInfo.APP_NAME,appName);
        }
        if (startTime != null) {
            queryWrapper.ge(ConfigInfo.GMT_MODIFIED,startTime);
        }
        if (endTime != null) {
            queryWrapper.le(ConfigInfo.GMT_MODIFIED,endTime);
        }
        IPage<ConfigInfo> configInfoIPage= configInfoService.page(page,queryWrapper);


        IPage<ConfigInfoWrapper> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(configInfoIPage.getCurrent());
        configInfoBaseIPage.setPages(configInfoIPage.getPages());
        configInfoBaseIPage.setSize(configInfoIPage.getSize());
        configInfoBaseIPage.setTotal(configInfoIPage.getTotal());
        configInfoBaseIPage.setRecords(configInfoIPage.getRecords().stream().map(vo->{ConfigInfoWrapper configInfoBase=new ConfigInfoWrapper(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    public List<ConfigInfo> findDeletedConfig(final Timestamp startTime,
                                              final Timestamp endTime) {
        List<Map<String, Object>> list = hisConfigInfoService.listMaps(new QueryWrapper<HisConfigInfo>().between(HisConfigInfo.GMT_MODIFIED,startTime,endTime));
        return convertDeletedConfig(list);
    }

    /**
     * 增加配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param srcIp             ip
     * @param srcUser           user
     * @param configInfo        info
     * @param time              time
     * @param configAdvanceInfo advance info
     * @return excute sql result
     */
    private Long addConfigInfoAtomic(final String srcIp, final String srcUser, final ConfigInfo configInfo,
                                     final Timestamp time,
                                     Map<String, Object> configAdvanceInfo) {
        final String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null
                : configInfo.getAppName();
        final String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null
                : configInfo.getTenantId();

        final String desc = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("schema");

        Long id = IdWorker.getId();

        final String md5Tmp = MD5.getInstance().getMD5String(configInfo.getContent());
        ConfigInfo configInfoData=new ConfigInfo();
        configInfoData.setId(id);
        configInfoData.setCUse(use);
        configInfoData.setCDesc(desc);
        configInfoData.setEffect(effect);
        configInfoData.setType(type);
        configInfoData.setCSchema(schema);
        configInfoData.setAppName(appNameTmp);
        configInfoData.setTenantId(tenantTmp);
        configInfoData.setSrcUser(srcUser);
        configInfoData.setSrcIp(srcIp);
        configInfoData.setGmtCreate(time);
        configInfoData.setGmtModified(time);
        configInfoData.setContent(configInfo.getContent());
        configInfoData.setMd5(md5Tmp);
        configInfoData.setGroupId(configInfo.getGroupId());
        configInfoData.setDataId(configInfo.getDataId());
        configInfoService.save(configInfoData);
        return id;
    }

    /**
     * 增加配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param configId id
     * @param tagName  tag
     * @param dataId   data id
     * @param group    group
     * @param tenant   tenant
     */
    public void addConfiTagRelationAtomic(long configId, String tagName, String dataId, String group, String tenant) {
        ConfigTagsRelation configTagsRelation=new ConfigTagsRelation();
        configTagsRelation.setDataId(dataId);
        configTagsRelation.setGroupId(group);
        configTagsRelation.setTenantId(tenant);
        configTagsRelation.setId(configId);
        configTagsRelation.setTagName(tagName);
        configTagsRelationService.save(configTagsRelation);
    }

    /**
     * 增加配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param configId   config id
     * @param configTags tags
     * @param dataId     dataId
     * @param group      group
     * @param tenant     tenant
     */
    public void addConfiTagsRelationAtomic(long configId, String configTags, String dataId, String group,
                                           String tenant) {
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            for (String tag : tagArr) {
                addConfiTagRelationAtomic(configId, tag, dataId, group, tenant);
            }
        }
    }

    public void removeTagByIdAtomic(long id) {
        configTagsRelationService.removeById(id);
    }

    public List<String> getConfigTagsByTenant(String tenant) {
        ConfigTagsRelation configTagsRelation=new ConfigTagsRelation();
        configTagsRelation.setTenantId(tenant);
        List<ConfigTagsRelation> list=configTagsRelationService.list(new QueryWrapper<>(configTagsRelation));
        return list.stream().map(vo->vo.getTagName()).collect(Collectors.toList());
    }

    public List<String> selectTagByConfig(String dataId, String group, String tenant) {
        ConfigTagsRelation configTagsRelation=new ConfigTagsRelation();
        configTagsRelation.setDataId(dataId);
        configTagsRelation.setGroupId(group);
        configTagsRelation.setTenantId(tenant);
        List<ConfigTagsRelation> list=configTagsRelationService.list(new QueryWrapper<>(configTagsRelation));
        return list.stream().map(vo->vo.getTagName()).collect(Collectors.toList());
    }

    /**
     * 删除配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId  dataId
     * @param group   group
     * @param tenant  tenant
     * @param srcIp   ip
     * @param srcUser user
     */
    private void removeConfigInfoAtomic(final String dataId, final String group, final String tenant,
                                        final String srcIp,
                                        final String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        configInfo.setTenantId(tenantTmp);
        configInfoService.remove(new QueryWrapper<>(configInfo));
    }

    /**
     * @author klw
     * @Description: Delete configuration; database atomic operation, minimum SQL action, no business encapsulation
     * @Date 2019/7/5 16:39
     * @Param [id]
     * @return void
     */
    private void removeConfigInfoByIdsAtomic(final List<Long> ids) {
        if(ids==null || ids.isEmpty()){
            return;
        }
        configInfoService.removeByIds(ids);
    }
    /**
     * 删除配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId  dataId
     * @param group   group
     * @param tenant  tenant
     * @param tag     tag
     * @param srcIp   ip
     * @param srcUser user
     */
    public void removeConfigInfoTag(final String dataId, final String group, final String tenant, final String tag,
                                    final String srcIp,
                                    final String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        String tagTmp = StringUtils.isBlank(tag) ? null : tag;
        ConfigInfoTag configInfoTag=new ConfigInfoTag();
        configInfoTag.setDataId(dataId);
        configInfoTag.setGroupId(group);
        configInfoTag.setTagId(tagTmp);
        configInfoTag.setTenantId(tenantTmp);
        configInfoTagService.remove(new QueryWrapper<>(configInfoTag));
    }

    /**
     * 更新配置;数据库原子操作，最小sql动作，无业务封装
     *
     * @param configInfo        config info
     * @param srcIp             ip
     * @param srcUser           user
     * @param time              time
     * @param configAdvanceInfo advance info
     */
    private void updateConfigInfoAtomic(final ConfigInfo configInfo, final String srcIp, final String srcUser,
                                        final Timestamp time, Map<String, Object> configAdvanceInfo) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        final String md5Tmp = MD5.getInstance().getMD5String(configInfo.getContent());
        String desc = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("desc");
        String use = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("use");
        String effect = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("effect");
        String type = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("type");
        String schema = configAdvanceInfo == null ? null : (String)configAdvanceInfo.get("schema");

        try {
            ConfigInfo configInfo1=new ConfigInfo();
            configInfo1.setCSchema(schema);
            configInfo1.setType(type);
            configInfo1.setEffect(effect);
            configInfo1.setCUse(use);
            configInfo1.setCDesc(desc);
            configInfo1.setContent(configInfo.getContent());
            configInfo1.setMd5(md5Tmp);
            configInfo1.setSrcIp(srcIp);
            configInfo1.setSrcUser(srcUser);
            configInfo1.setGmtModified(time);
            configInfo1.setAppName(appNameTmp);
            ConfigInfo configInfoQuery=new ConfigInfo();
            configInfoQuery.setDataId(configInfo.getDataId());
            configInfoQuery.setGroupId(configInfo.getGroupId());
            configInfoQuery.setTenantId(tenantTmp);
            configInfoService.update(configInfo1,new QueryWrapper<>(configInfoQuery));
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 查询配置信息；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return config info
     */
    public ConfigInfo findConfigInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfo configInfo=new ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        configInfo.setTenantId(tenantTmp);
        return  configInfoService.getOne(new QueryWrapper<>(configInfo));
    }

    /**
     * @author klw
     * @Description: find ConfigInfo by ids
     * @Date 2019/7/5 16:37
     * @Param [ids]
     * @return java.util.List<com.alibaba.nacos.config.server.model.ConfigInfo>
     */
    public Collection<ConfigInfo> findConfigInfosByIds(final List<Long> ids) {
        if(ids==null || ids.isEmpty()){
            return null;
        }
        return configInfoService.listByIds(ids);

    }

    /**
     * 查询配置信息；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return advance info
     */
    public ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);
            ConfigInfo configInfo=new ConfigInfo();
            configInfo.setDataId(dataId);
            configInfo.setGroupId(group);
            configInfo.setTenantId(tenantTmp);
            ConfigInfo configInfo1=configInfoService.getOne(new QueryWrapper<>(configInfo));

            ConfigAdvanceInfo configAdvance =new ConfigAdvanceInfo(configInfo1);
            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(",").append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // 表明数据不存在, 返回null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 查询配置信息；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return advance info
     */
    public ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);
            ConfigInfo configInfo=new ConfigInfo();
            configInfo.setDataId(dataId);
            configInfo.setGroupId(group);
            configInfo.setTenantId(tenantTmp);
            ConfigInfo configInfo1=configInfoService.getOne(new QueryWrapper<>(configInfo));

            if(configInfo1==null){
                return null;
            }
            ConfigAllInfo configAdvance =new ConfigAllInfo(configInfo1);
            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(",").append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // 表明数据不存在, 返回null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * 更新变更记录；数据库原子操作，最小sql动作，无业务封装
     *
     * @param id         id
     * @param configInfo config info
     * @param srcIp      ip
     * @param srcUser    user
     * @param time       time
     * @param ops        ops type
     */
    private void insertConfigHistoryAtomic(long id, ConfigInfo configInfo, String srcIp, String srcUser,
                                           final Timestamp time, String ops) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? null : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenantId()) ? null : configInfo.getTenantId();
        final String md5Tmp = MD5.getInstance().getMD5String(configInfo.getContent());
        HisConfigInfo hisConfigInfo=new HisConfigInfo();
        hisConfigInfo.setId(id);
        hisConfigInfo.setDataId(configInfo.getDataId());
        hisConfigInfo.setGroupId(configInfo.getGroupId());
        hisConfigInfo.setTenantId(tenantTmp);
        hisConfigInfo.setAppName(appNameTmp);
        hisConfigInfo.setMd5(md5Tmp);
        hisConfigInfo.setSrcIp(srcIp);
        hisConfigInfo.setSrcUser(srcUser);
        hisConfigInfo.setGmtCreate(time);
        hisConfigInfo.setOpType(ops);
        hisConfigInfo.setContent(configInfo.getContent());
        hisConfigInfoService.save(hisConfigInfo);
    }

    /**
     * list配置的历史变更记录
     *
     * @param dataId   data Id
     * @param group    group
     * @param tenant   tenant
     * @param pageNo   no
     * @param pageSize size
     * @return history info
     */
    public IPage<ConfigHistoryInfo> findConfigHistory(String dataId, String group, String tenant, int pageNo,
                                                      int pageSize) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        IPage page = new Page<>();
        page.setSize(pageSize);
        page.setCurrent(pageNo);
        HisConfigInfo configHistoryInfo=new HisConfigInfo();
        configHistoryInfo.setDataId(dataId);
        configHistoryInfo.setGroupId(group);
        configHistoryInfo.setTenantId(tenantTmp);
        IPage<HisConfigInfo> historyInfoPage= hisConfigInfoService.page(page,new QueryWrapper<>(configHistoryInfo));

        IPage<ConfigHistoryInfo> configInfoBaseIPage=new Page<>();
        configInfoBaseIPage.setCurrent(historyInfoPage.getCurrent());
        configInfoBaseIPage.setPages(historyInfoPage.getPages());
        configInfoBaseIPage.setSize(historyInfoPage.getSize());
        configInfoBaseIPage.setTotal(historyInfoPage.getTotal());
        configInfoBaseIPage.setRecords(historyInfoPage.getRecords().stream().map(vo->{ConfigHistoryInfo configInfoBase=new ConfigHistoryInfo(vo);return configInfoBase;}).collect(Collectors.toList()));
        return configInfoBaseIPage;
    }

    /**
     * 增加配置；数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId  dataId
     * @param group   group
     * @param appName appName
     * @param date    date
     */
    private void addConfigSubAtomic(final String dataId, final String group, final String appName,
                                    final Timestamp date) {
        final String appNameTmp = appName == null ? "" : appName;
        AppConfigdataRelationSubs appConfigdataRelationSubs=new AppConfigdataRelationSubs();
        appConfigdataRelationSubs.setAppName(appNameTmp);
        appConfigdataRelationSubs.setGroupId(group);
        appConfigdataRelationSubs.setGmtModified(date);
        appConfigdataRelationSubs.setDataId(dataId);
        appConfigdataRelationSubsService.save(appConfigdataRelationSubs);
    }

    /**
     * 更新配置;数据库原子操作，最小sql动作，无业务封装
     *
     * @param dataId  data Id
     * @param group   group
     * @param appName app name
     * @param time    time
     */
    private void updateConfigSubAtomic(final String dataId, final String group, final String appName,
                                       final Timestamp time) {
        final String appNameTmp = appName == null ? "" : appName;
        AppConfigdataRelationSubs appConfigdataRelationSubs=new AppConfigdataRelationSubs();
        appConfigdataRelationSubs.setGmtModified(time);
        appConfigdataRelationSubsService.update(appConfigdataRelationSubs,new QueryWrapper<AppConfigdataRelationSubs>().eq(AppConfigdataRelationSubs.DATA_ID,dataId)
                .eq(AppConfigdataRelationSubs.GROUP_ID,group).eq(AppConfigdataRelationSubs.APP_NAME,appNameTmp));

    }

    public ConfigHistoryInfo detailConfigHistory(Long nid) {
        HisConfigInfo hisConfigInfo=hisConfigInfoService.getById(nid);
        if(hisConfigInfo==null){
            return null;
        }
        ConfigHistoryInfo configHistoryInfo=new ConfigHistoryInfo(hisConfigInfo);
        return  configHistoryInfo;
    }

    /**
     * insert tenant info
     *
     * @param kp         kp
     * @param tenantId   tenant Id
     * @param tenantName tenant name
     * @param tenantDesc tenant description
     * @param time       time
     */
    public void insertTenantInfoAtomic(String kp, String tenantId, String tenantName, String tenantDesc,
                                       String createResoure, final long time) {
        TenantInfo tenantInfo=new TenantInfo();
        tenantInfo.setTenantId(tenantId);
        tenantInfo.setTenantName(tenantName);
        tenantInfo.setTenantDesc(tenantDesc);
        tenantInfo.setGmtModified(time);
        tenantInfo.setGmtCreate(time);
        tenantInfo.setCreateSource(createResoure);
        tenantInfo.setKp(kp);
        tenantInfoService.save(tenantInfo);
    }

    /**
     * Update tenantInfo showname
     *
     * @param kp         kp
     * @param tenantId   tenant Id
     * @param tenantName tenant name
     * @param tenantDesc tenant description
     */
    public void updateTenantNameAtomic(String kp, String tenantId, String tenantName, String tenantDesc) {
        TenantInfo tenantInfo=new TenantInfo();
        tenantInfo.setTenantName(tenantName);
        tenantInfo.setTenantDesc(tenantDesc);
        tenantInfo.setGmtModified(System.currentTimeMillis());
        tenantInfo.setTenantId(tenantId);
        tenantInfoService.update(tenantInfo,new QueryWrapper<TenantInfo>().eq(TenantInfo.KP,kp));
    }

    public List<TenantInfo> findTenantByKp(String kp) {
        List<TenantInfo> tenantInfos= tenantInfoService.list(new QueryWrapper<TenantInfo>().eq(TenantInfo.KP,kp));
        return tenantInfos;
    }

    public TenantInfo findTenantByKp(String kp, String tenantId) {
        TenantInfo tenantInfop=new TenantInfo();
        tenantInfop.setKp(kp);
        tenantInfop.setTenantId(tenantId);
        TenantInfo tenantInfo= tenantInfoService.getOne(new QueryWrapper<>(tenantInfop));
        return tenantInfo;
    }

    public void removeTenantInfoAtomic(final String kp, final String tenantId) {
        TenantInfo tenantInfop=new TenantInfo();
        tenantInfop.setKp(kp);
        tenantInfop.setTenantId(tenantId);
        tenantInfoService.remove(new QueryWrapper<>(tenantInfop));
    }

    public Users findUserByUsername(String username) {
        Users  user=usersService.getOne(new QueryWrapper<Users>().eq(Users.USERNAME,username));
        return user;
    }
    /**
     * 更新用户密码
     */
    public void updateUserPassword(String username, String password) {
        try {
            Users users=new Users();
            users.setPassword(password);
            usersService.update(users,new QueryWrapper<Users>().eq(Users.USERNAME,username));
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    private List<ConfigInfo> convertDeletedConfig(List<Map<String, Object>> list) {
        List<ConfigInfo> configs = new ArrayList<ConfigInfo>();
        for (Map<String, Object> map : list) {
            String dataId = (String)map.get("DATA_ID");
            String group = (String)map.get("GROUP_ID");
            String tenant = (String)map.get("TENANT_ID");
            ConfigInfo config = new ConfigInfo();
            config.setDataId(dataId);
            config.setGroupId(group);
            config.setTenantId(tenant);
            configs.add(config);
        }
        return configs;
    }

    private List<ConfigInfoWrapper> convertChangeConfig(
            List<Map<String, Object>> list) {
        List<ConfigInfoWrapper> configs = new ArrayList<ConfigInfoWrapper>();
        for (Map<String, Object> map : list) {
            String dataId = (String)map.get("DATA_ID");
            String group = (String)map.get("GROUP_ID");
            String tenant = (String)map.get("TENANT_ID");
            String content = (String)map.get("CONTENT");
            long mTime = ((Timestamp)map.get("GMT_MODIFIED")).getTime();
            ConfigInfoWrapper config = new ConfigInfoWrapper();
            config.setDataId(dataId);
            config.setGroupId(group);
            config.setTenantId(tenant);
            config.setContent(content);
            config.setLastModified(mTime);
            configs.add(config);
        }
        return configs;
    }

    /**
     * 获取所有的配置的Md5值，通过分页方式获取。
     *
     * @return
     */
    public List<ConfigInfoWrapper> listAllGroupKeyMd5() {
        final int pageSize = 10000;
        int totalCount = configInfoCount();
        int pageCount = (int)Math.ceil(totalCount * 1.0 / pageSize);
        List<ConfigInfoWrapper> allConfigInfo = new ArrayList<ConfigInfoWrapper>();
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            List<ConfigInfoWrapper> configInfoList = listGroupKeyMd5ByPage(pageNo, pageSize);
            allConfigInfo.addAll(configInfoList);
        }
        return allConfigInfo;
    }

    private List<ConfigInfoWrapper> listGroupKeyMd5ByPage(int pageNo, int pageSize) {
        Page page=new Page();
        page.setCurrent(pageNo);
        page.setSize(pageSize);

        QueryWrapper queryWrapper= new QueryWrapper<ConfigInfo>();
        IPage<ConfigInfo> configInfoIPage= configInfoService.page(page,queryWrapper);
        return configInfoIPage.getRecords().stream().map(vo->{ConfigInfoWrapper configInfoWrapper=new ConfigInfoWrapper(vo);return configInfoWrapper;}).collect(Collectors.toList());
    }

    private String generateLikeArgument(String s) {
        String fuzzySearchSign = "\\*";
        String sqlLikePercentSign = "%";
        if (s.contains(PATTERN_STR)) { return s.replaceAll(fuzzySearchSign, sqlLikePercentSign); } else {
            return s;
        }
    }

    public ConfigInfoWrapper queryConfigInfo(final String dataId, final String group, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;
        ConfigInfo configInfo=new  ConfigInfo();
        configInfo.setDataId(dataId);
        configInfo.setGroupId(group);
        configInfo.setTenantId(tenantTmp);
        ConfigInfo configInfo1= configInfoService.getOne(new QueryWrapper<>(configInfo));
        ConfigInfoWrapper configInfoWrapper=new ConfigInfoWrapper(configInfo1);
        return configInfoWrapper;
    }

    public boolean isExistTable(String tableName) {
        try{
            if(BETA_TABLE_NAME.equalsIgnoreCase(tableName)){
                configInfoBetaService.count();
            }else if(TAG_TABLE_NAME.equalsIgnoreCase(tableName)){
                configInfoTagService.count();
            }
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public Boolean completeMd5() {
        defaultLog.info("[start completeMd5]");
        int perPageSize = 1000;
        int rowCount = configInfoCount();
        int pageCount = (int)Math.ceil(rowCount * 1.0 / perPageSize);
        int actualRowCount = 0;
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            IPage<PersistService.ConfigInfoWrapper> page = findAllConfigInfoForDumpAll(
                    pageNo, perPageSize);
            if (page != null) {
                for (PersistService.ConfigInfoWrapper cf : page.getRecords()) {
                    String md5InDb = cf.getMd5();
                    final String content = cf.getContent();
                    final String tenant = cf.getTenantId();
                    final String md5 = MD5.getInstance().getMD5String(
                            content);
                    if (StringUtils.isBlank(md5InDb)) {
                        try {
                            updateMd5(cf.getDataId(), cf.getGroupId(), tenant, md5, new Timestamp(cf.getLastModified()));
                        } catch (Exception e) {
                            LogUtil.defaultLog
                                    .error("[completeMd5-error] datId:{} group:{} lastModified:{}",
                                            new Object[] {
                                                    cf.getDataId(),
                                                    cf.getGroupId(),
                                                    new Timestamp(cf
                                                            .getLastModified())});
                        }
                    } else {
                        if (!md5InDb.equals(md5)) {
                            try {
                                updateMd5(cf.getDataId(), cf.getGroupId(), tenant, md5,
                                        new Timestamp(cf.getLastModified()));
                            } catch (Exception e) {
                                LogUtil.defaultLog.error("[completeMd5-error] datId:{} group:{} lastModified:{}",
                                        new Object[] {cf.getDataId(), cf.getGroupId(),
                                                new Timestamp(cf.getLastModified())});
                            }
                        }
                    }
                }

                actualRowCount += page.getRecords().size();
                defaultLog.info("[completeMd5] {} / {}", actualRowCount, rowCount);
            }
        }
        return true;
    }

    /**
     * query all configuration information according to group, appName, tenant (for export)
     *
     * @param group
     * @return Collection of ConfigInfo objects
     */
    public List<ConfigInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant,
                                                     final String appName, final List<Long> ids) {
        String tenantTmp = StringUtils.isBlank(tenant) ? null : tenant;

        ConfigInfo configInfo1=new ConfigInfo();
        configInfo1.setTenantId(tenantTmp);
        if (!CollectionUtils.isEmpty(ids)) {
            return configInfoService.list(new QueryWrapper<ConfigInfo>().in(ConfigInfo.ID,ids));
        } else {
            if (StringUtils.isNotBlank(appName)) {
                configInfo1.setAppName(appName);
            }
            if (StringUtils.isNotBlank(group)) {
                configInfo1.setGroupId(group);
            }
            if (StringUtils.isNotBlank(dataId)) {
                return  configInfoService.list(new QueryWrapper<>(configInfo1).like(ConfigInfo.DATA_ID,dataId));
            }else {
                return  configInfoService.list(new QueryWrapper<>(configInfo1));
            }
        }
    }

    /**
     * batch operation,insert or update
     * the format of the returned:
     * succCount: number of successful imports
     * skipCount: number of import skips (only with skip for the same configs)
     * failData: import failed data (only with abort for the same configs)
     * skipData: data skipped at import  (only with skip for the same configs)
     */
    public Map<String, Object> batchInsertOrUpdate(List<ConfigInfo> configInfoList, String srcUser, String srcIp,
                                                   Map<String, Object> configAdvanceInfo, Timestamp time, boolean notify, SameConfigPolicy policy) throws NacosException {
        int succCount = 0;
        int skipCount = 0;
        List<Map<String, String>> failData = null;
        List<Map<String, String>> skipData = null;

        for (int i = 0; i < configInfoList.size(); i++) {
            ConfigInfo configInfo = configInfoList.get(i);
            try {
                ParamUtils.checkParam(configInfo.getDataId(), configInfo.getGroupId(), "datumId", configInfo.getContent());
            } catch (NacosException e) {
                defaultLog.error("data verification failed", e);
                throw e;
            }
            ConfigInfo configInfo2Save = new ConfigInfo(configInfo.getDataId(), configInfo.getGroupId(),
                    configInfo.getTenantId(), configInfo.getAppName(), configInfo.getContent());

            // simple judgment of file type based on suffix
            String type = null;
            if (configInfo.getDataId().contains(SPOT)) {
                String extName = configInfo.getDataId().substring(configInfo.getDataId().lastIndexOf(SPOT) + 1).toLowerCase();
                try{
                    type = FileTypeEnum.valueOf(extName).getFileType();
                }catch (Exception ex){
                    type = FileTypeEnum.TEXT.getFileType();
                }
            }
            if (configAdvanceInfo == null) {
                configAdvanceInfo = new HashMap<>(16);
            }
            configAdvanceInfo.put("type", type);
            try {
                addConfigInfo(srcIp, srcUser, configInfo2Save, time, configAdvanceInfo, notify);
                succCount++;
            } catch (DataIntegrityViolationException ive) {
                // uniqueness constraint conflict
                if (SameConfigPolicy.ABORT.equals(policy)) {
                    failData = new ArrayList<>();
                    skipData = new ArrayList<>();
                    Map<String, String> faileditem = new HashMap<>(2);
                    faileditem.put("dataId", configInfo2Save.getDataId());
                    faileditem.put("group", configInfo2Save.getGroupId());
                    failData.add(faileditem);
                    for (int j = (i + 1); j < configInfoList.size(); j++) {
                        ConfigInfo skipConfigInfo = configInfoList.get(j);
                        Map<String, String> skipitem = new HashMap<>(2);
                        skipitem.put("dataId", skipConfigInfo.getDataId());
                        skipitem.put("group", skipConfigInfo.getGroupId());
                        skipData.add(skipitem);
                    }
                    break;
                } else if (SameConfigPolicy.SKIP.equals(policy)) {
                    skipCount++;
                    if (skipData == null) {
                        skipData = new ArrayList<>();
                    }
                    Map<String, String> skipitem = new HashMap<>(2);
                    skipitem.put("dataId", configInfo2Save.getDataId());
                    skipitem.put("group", configInfo2Save.getGroupId());
                    skipData.add(skipitem);
                } else if (SameConfigPolicy.OVERWRITE.equals(policy)) {
                    succCount++;
                    updateConfigInfo(configInfo2Save, srcIp, srcUser, time, configAdvanceInfo, notify);
                }
            }
        }
        Map<String, Object> result = new HashMap<>(4);
        result.put("succCount", succCount);
        result.put("skipCount", skipCount);
        if (failData != null && !failData.isEmpty()) {
            result.put("failData", failData);
        }
        if (skipData != null && !skipData.isEmpty()) {
            result.put("skipData", skipData);
        }
        return result;
    }


    /**
     * query tenantInfo (namespace) existence based by tenantId
     *
     * @param tenantId
     * @return count by tenantId
     */
    public int tenantInfoCountByTenantId(String tenantId) {
        Assert.hasText(tenantId, "tenantId can not be null");
        TenantInfo configInfo1=new TenantInfo();
        configInfo1.setTenantId(tenantId);
        Integer result =tenantInfoService.count(new QueryWrapper<>(configInfo1));
        if (result == null) {
            return 0;
        }
        return result.intValue();
    }

    private static String PATTERN_STR = "*";
    private final static int QUERY_LIMIT_SIZE = 50;

    public final static String BETA_TABLE_NAME = "config_info_beta";
    public final static String TAG_TABLE_NAME = "config_info_tag";

}
