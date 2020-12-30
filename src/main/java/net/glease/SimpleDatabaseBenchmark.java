/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package net.glease;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.stream.IntStream;

@State(Scope.Thread)
public class SimpleDatabaseBenchmark
{
    @Param({"3000"})
    int EXPECTED_TABLE_SIZE = 3000;
    SimpleDatabase<String> db;
    int[] keys;
    @Param({"3", "10", "100", "1000"})
    int size;


    @Setup(Level.Iteration)
    public void setup() {
        db = new SimpleDatabase<String>() {
        };
        for (int i = 0; i < EXPECTED_TABLE_SIZE; i++) {
            db.add(i, Integer.toString(i));
        }
        db.bake();
        this.keys = IntStream.range(0, size).map(s -> EXPECTED_TABLE_SIZE / size * (s + 1) - 1).toArray();
    }

    @Benchmark
    public List<DBEntry<String>> sortMergeJoin() {
        return db.bulkLookup(keys);
    }

    @Benchmark
    public List<DBEntry<String>> naive() {
        return db.bulkLookupNaive(keys);
    }

    @Benchmark
    public List<DBEntry<String>> arrayCache() {
        return db.bulkLookupBaked(keys);
    }

    public static void main(String[] args) throws RunnerException {
        String[] sizeList = IntStream.range(1, 3000)
                .filter(i -> i % ((int)Math.pow(10, (int) Math.log10(i))) == 0)
                .mapToObj(Integer::toString)
                .toArray(String[]::new);
        ChainedOptionsBuilder opt = new OptionsBuilder()
                .include(SimpleDatabaseBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(2)
                .warmupIterations(4)
                .measurementIterations(8)
                .resultFormat(ResultFormatType.JSON)
                .param("size", sizeList);
        if (args.length > 0)
            opt.result(args[0]);
        new Runner(opt.build()).run();
    }
}
