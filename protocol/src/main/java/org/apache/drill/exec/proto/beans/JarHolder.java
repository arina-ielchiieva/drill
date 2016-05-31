/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.dyuproject.protostuff.ByteString;
import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class JarHolder implements Externalizable, Message<JarHolder>, Schema<JarHolder>
{

    public static Schema<JarHolder> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static JarHolder getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final JarHolder DEFAULT_INSTANCE = new JarHolder();

    
    private String name;
    private List<ByteString> content;

    public JarHolder()
    {
        
    }

    public JarHolder(
        String name
    )
    {
        this.name = name;
    }

    // getters and setters

    // name

    public String getName()
    {
        return name;
    }

    public JarHolder setName(String name)
    {
        this.name = name;
        return this;
    }

    // content

    public List<ByteString> getContentList()
    {
        return content;
    }

    public JarHolder setContentList(List<ByteString> content)
    {
        this.content = content;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<JarHolder> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public JarHolder newMessage()
    {
        return new JarHolder();
    }

    public Class<JarHolder> typeClass()
    {
        return JarHolder.class;
    }

    public String messageName()
    {
        return JarHolder.class.getSimpleName();
    }

    public String messageFullName()
    {
        return JarHolder.class.getName();
    }

    public boolean isInitialized(JarHolder message)
    {
        return 
            message.name != null;
    }

    public void mergeFrom(Input input, JarHolder message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.name = input.readString();
                    break;
                case 2:
                    if(message.content == null)
                        message.content = new ArrayList<ByteString>();
                    message.content.add(input.readBytes());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, JarHolder message) throws IOException
    {
        if(message.name == null)
            throw new UninitializedMessageException(message);
        output.writeString(1, message.name, false);

        if(message.content != null)
        {
            for(ByteString content : message.content)
            {
                if(content != null)
                    output.writeBytes(2, content, true);
            }
        }
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "name";
            case 2: return "content";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("name", 1);
        __fieldMap.put("content", 2);
    }
    
}
