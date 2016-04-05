<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#include "*/generic.ftl">
<#macro page_head>
</#macro>

<#macro page_body>
<a href="/queries">back</a><br/>
<div class="page-header">
</div>

    <#list groups?keys as groupKey>
    ${groupKey}
        <#list groups.get(groupKey) as item>
        ${item}
        </#list>
    </#list>

<#if (model?size > 0)>
<div class="table-responsive">
    <table class="table table-hover">
            <thead>
            <td>Name</td>
            <td>Size</td>
            <td>Last Modified</td>
            <td>Location</td>
            </thead>
        <tbody>
            <#list model?keys as loc>
                <#list model[loc] as log>
                <tr>
                    <td>
                        <a href="/log/${log.getName()}">
                            <div style="height:100%;width:100%;white-space:pre-line">${log.getName()}</div>
                        </a>
                    </td>
                    <td>
                        <div style="height:100%;width:100%;white-space:pre-line">${log.getSize()}</div>
                    </td>
                    <td>
                        <div style="height:100%;width:100%;white-space:pre-line">${log.getLastModified()}</div>
                    </td>
                    <td>
                        <div style="height:100%;width:100%;white-space:pre-line">${loc}</div>
                    </td>
                </tr>
                </#list>
            </#list>
        </tbody>
    </table>
</div>
<#else>
<div id="message" class="alert alert-info">
    <strong>No logs are available.</strong>
</div>
</#if>
</#macro>

<@page_html/>