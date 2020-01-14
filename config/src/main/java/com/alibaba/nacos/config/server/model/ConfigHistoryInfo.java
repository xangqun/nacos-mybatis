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
package com.alibaba.nacos.config.server.model;

import com.alibaba.nacos.config.server.mybatis.domain.entity.HisConfigInfo;
import com.alibaba.nacos.core.json.LongJsonDeserializer;
import com.alibaba.nacos.core.json.LongJsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.sql.Timestamp;

/**
 * history Info
 *
 * @author Nacos
 */
public class ConfigHistoryInfo {

    /**
     * id, nid, data_id, group_id, content, md5, gmt_create, gmt_modified, （配置创建时间，配置变更时间） src_user, src_ip, (变更操作者)
     * op_type（变更操作类型）
     */
    @JsonSerialize(using = LongJsonSerializer.class)
    @JsonDeserialize(using = LongJsonDeserializer.class)
    private Long id;
    /**
     * 上次改动历史的id
     */
    private long lastId = -1;
    @JsonSerialize(using = LongJsonSerializer.class)
    @JsonDeserialize(using = LongJsonDeserializer.class)
    private Long nid;
    private String dataId;
    private String group;
    private String tenant;
    private String appName;
    private String md5;
    private String content;
    private String srcIp;
    private String srcUser;
    /**
     * 操作类型, 包括插入、更新、删除
     */
    private String opType;
    private Timestamp createdTime;
    private Timestamp lastModifiedTime;


    public ConfigHistoryInfo(HisConfigInfo hisConfigInfo){
        this.id=hisConfigInfo.getId();
        this.dataId=hisConfigInfo.getDataId();
        this.group=hisConfigInfo.getGroupId();
        this.tenant=hisConfigInfo.getTenantId();
        this.appName=hisConfigInfo.getAppName();
        this.md5=hisConfigInfo.getMd5();
        this.srcIp=hisConfigInfo.getSrcIp();
        this.srcUser=hisConfigInfo.getSrcUser();
        this.opType=hisConfigInfo.getOpType();
        this.createdTime=hisConfigInfo.getGmtCreate()==null?null:new Timestamp(hisConfigInfo.getGmtCreate().getTime());
        this.lastModifiedTime=hisConfigInfo.getGmtModified()==null?null:new Timestamp(hisConfigInfo.getGmtModified().getTime());
        this.content=hisConfigInfo.getContent();
        /* 20190919 这里缺少了nid，唯一识别一个规则的字段，需要加入 */
        this.nid =hisConfigInfo.getNid();
    }


    public Long getNid() {
        return nid;
    }

    public void setNid(Long nid) {
        this.nid = nid;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public long getLastId() {
        return lastId;
    }

    public void setLastId(long lastId) {
        this.lastId = lastId;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public String getSrcUser() {
        return srcUser;
    }

    public void setSrcUser(String srcUser) {
        this.srcUser = srcUser;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public Timestamp getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Timestamp createdTime) {
        this.createdTime = createdTime;
    }

    public Timestamp getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(Timestamp lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }
}
