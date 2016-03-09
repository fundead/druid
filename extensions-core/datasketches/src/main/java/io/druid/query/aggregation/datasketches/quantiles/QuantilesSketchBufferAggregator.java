/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.sketches.quantiles.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class QuantilesSketchBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final int size;

  private final Map<Integer, Union> quantilesSketches = new HashMap<>(); //position in BB -> QuantilesSketch Object

  public QuantilesSketchBufferAggregator(ObjectColumnSelector selector, int size, int maxIntermediateSize)
  {
    this.selector = selector;
    this.size = size;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    quantilesSketches.put(position, QuantilesSketchUtils.buildUnion(size));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    QuantilesSketchAggregator.updateQuantilesSketch(getQuantilesSketchUnion(buf, position), selector.get());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getQuantilesSketchUnion(buf, position).getResult();
  }

  private Union getQuantilesSketchUnion(ByteBuffer buf, int position)
  {
    Union union = quantilesSketches.get(position);
    if (union == null) {
      throw new IllegalStateException("failed to find quantile sketch union.");
    }
    return union;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    quantilesSketches.clear();
  }

}
