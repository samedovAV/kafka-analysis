/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.partition.PartitionListener;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import com.samedov.annotation.Prove;
import com.samedov.annotation.Complexity;

/**
 * Abstraction for partition metadata operations.
 */
public interface PartitionMetadataProvider {

    /**
     * Resolve the offset for the earliest timestamp.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    long offsetForEarliestTimestamp(TopicIdPartition topicIdPartition, int leaderEpoch);

    /**
     * Resolve the offset for the latest timestamp.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    long offsetForLatestTimestamp(TopicIdPartition topicIdPartition, int leaderEpoch);

    /**
     * Resolve the offset for a specific timestamp.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    long offsetForTimestamp(TopicIdPartition topicIdPartition, long timestamp, int leaderEpoch);

    /**
     * Get the end offset metadata for partition.
     *
     * @return The end offset metadata based on the given fetch isolation.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    LogOffsetMetadata endOffsetMetadata(TopicIdPartition topicIdPartition, FetchIsolation isolation);

    /**
     * Get the leader epoch for a partition.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    int leaderEpoch(TopicIdPartition topicIdPartition);

    /**
     * Register a partition listener for state change notifications.
     *
     * @return true if the listener was successfully added.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    boolean addPartitionListener(TopicIdPartition topicIdPartition, PartitionListener listener);

    /**
     * Remove a previously registered partition listener.
     */
    @Prove(complexity = Complexity.O_1, n = "", count = {})
    void removePartitionListener(TopicIdPartition topicIdPartition, PartitionListener listener);
}
