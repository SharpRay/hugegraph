/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.util.collection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.collections.impl.list.mutable.FastList;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.type.define.CollectionImplType;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

public class IdList extends ArrayList<Id> {

    private LongArrayList numberIds;
    private List<Id> nonNumberIds;

    public IdList(CollectionImplType type) {
        this.numberIds = new LongArrayList();
        switch (type) {
            case JCF:
                this.nonNumberIds = new ArrayList<>();
                break;
            case EC:
                this.nonNumberIds = new FastList<>();
                break;
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    @Override
    public int size() {
        return this.numberIds.size() + this.nonNumberIds.size();
    }

    @Override
    public boolean isEmpty() {
        return this.numberIds.isEmpty() && this.nonNumberIds.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof IdGenerator.LongId) {
            return this.numberIds.contains(((IdGenerator.LongId) o).longValue());
        } else {
            return this.nonNumberIds.contains(o);
        }
    }

    @Override
    public Iterator<Id> iterator() {
        ExtendableIterator<Id> iterator = new ExtendableIterator<>();
        iterator.extend(this.nonNumberIds.iterator());
        EcLongIterator iter = new EcLongIterator(this.numberIds.iterator());
        iterator.extend(new MapperIterator<>(iter, IdGenerator::of));
        return iterator;
    }

    public boolean add(Id id) {
        if (id instanceof IdGenerator.LongId) {
            this.numberIds.add(((IdGenerator.LongId) id).longValue());
            return true;
        } else {
            return this.nonNumberIds.add(id);
        }
    }

    @Override
    public boolean remove(Object o) {
        super.remove(o);
        if (o instanceof IdGenerator.LongId) {
            long id = ((IdGenerator.LongId) o).longValue();
            return this.numberIds.removeFirst(id) >= 0;
        } else {
            return this.nonNumberIds.remove(o);
        }
    }

    @Override
    public void clear() {
        this.numberIds.clear();
        this.nonNumberIds.clear();
    }

    private static class EcLongIterator implements Iterator<Long> {

        private Iterator<LongCursor> iterator;

        public EcLongIterator(Iterator<LongCursor> iter) {
            this.iterator = iter;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Long next() {
            return this.iterator.next().value;
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }
    }
}
