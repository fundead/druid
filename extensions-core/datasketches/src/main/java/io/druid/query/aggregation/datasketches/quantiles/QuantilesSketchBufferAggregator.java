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

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.QuantilesSketch;
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
  private final int maxOffheapSize;

  private NativeMemory nm;

  private final Map<Integer, QuantilesSketch> quantilesSketches = new HashMap<>(); //position in BB -> Quantiles sketch

  public QuantilesSketchBufferAggregator(ObjectColumnSelector selector, int size, int maxOffheapSize)
  {
    this.selector = selector;
    this.size = size;
    this.maxOffheapSize = maxOffheapSize;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    if (nm == null) {
      nm = new NativeMemory(buf);
    }

    saveQuantilesSketchUnion(buf, position, QuantilesSketchUtils.buildUnion(size));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Union union = getQuantilesSketchUnion(buf, position);
    QuantilesSketchAggregator.updateQuantilesSketch(union, selector.get());
    saveQuantilesSketchUnion(buf, position, union);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getQuantilesSketchUnion(buf, position).getResult();
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

  //if sketch size < maxOffheapSize then it is stored in input ByteBuffer or else it is stored
  //in the qualtilesSketches Map object. First byte in the ByteBuffer at offset "position" is reserved
  //to flag whether sketch object is stored off-heap or on-heap.
  private void saveQuantilesSketchUnion(ByteBuffer buf, int position, Union union)
  {
    QuantilesSketch sketch = union.getResultAndReset();
    if (sketch.getStorageBytes() < maxOffheapSize) {
      nm.putByte(position, (byte) 0);
      Memory mem = new MemoryRegion(nm, position+1, maxOffheapSize-1);
      sketch.putMemory(mem);
    } else {
      nm.putByte(position, (byte) 1);
      quantilesSketches.put(position, sketch);
    }
  }

  private Union getQuantilesSketchUnion(ByteBuffer buf, int position)
  {
    if (nm.getByte(position) == 0) {
      Memory mem = new MemoryRegion(nm, position+1, maxOffheapSize-1);
      return Union.builder().build(mem);
    } else {
      Union union = Union.builder().build(quantilesSketches.get(position));
      if (union == null) {
        throw new IllegalStateException("failed to find quantile sketch union.");
      }
      return union;
    }
  }
}
