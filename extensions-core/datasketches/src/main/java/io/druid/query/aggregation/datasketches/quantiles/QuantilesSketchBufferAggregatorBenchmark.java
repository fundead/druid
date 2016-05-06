/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.sketches.quantiles.QuantilesSketch;
import io.druid.segment.ObjectColumnSelector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 */
@State(Scope.Benchmark)
public class QuantilesSketchBufferAggregatorBenchmark
{
  private final Random random = new Random(123456789l);

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void testit(Blackhole blackhole) throws Exception
  {
//    final int maxOffheapSize = 1<<18;
    final int maxOffheapSize = 1;
    QuantilesSketchBufferAggregator aggregator = new QuantilesSketchBufferAggregator(
        new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object get()
          {
            return random.nextDouble();
          }
        },
        1024,
        maxOffheapSize
    );

    ByteBuffer bb = ByteBuffer.allocate(maxOffheapSize);

    aggregator.init(bb, 0);
    int n = 1000000;
    for (int i = 0; i < n; i++) {
      aggregator.aggregate(bb, 0);
    }

    blackhole.consume(((QuantilesSketch) aggregator.get(bb, 0)).getN());
  }
}
