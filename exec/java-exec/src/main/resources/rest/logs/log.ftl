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
<a href="/logs">back</a><br/>
<div class="page-header">
</div>
</div>
<h3>${model.getName()}</h3>
<a href="/log/download/${model.getName()}">
    <div style="height:100%;width:100%;white-space:pre-line">${model.getName()}</div>
</a>
<pre>
    <#list model.getLines() as line>
        ${line}
    </#list>
</pre>
</div>
<#--<script>
    var update = function() {
        $.get("/log/${model.getName()}", function(data) {
            $("#mainDiv").html("<pre>" + data + "</pre>");
        });
    };

    update();
    setInterval(update, 2000);
</script>-->
</#macro>

<@page_html/>