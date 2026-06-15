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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;
import com.samedov.annotation.Prove;
import com.samedov.annotation.Complexity;

public class DeleteRecordsPartitionStatus {
    private final long requiredOffset;
    private final DeleteRecordsPartitionResult responseStatus;
    private volatile boolean acksPending;

    public DeleteRecordsPartitionStatus(long requiredOffset, DeleteRecordsPartitionResult responseStatus) {
        this.requiredOffset = requiredOffset;
        this.responseStatus = responseStatus;
        this.acksPending = false;
    }

    @Prove(complexity = Complexity.O_1, n = "", count = {})
    public boolean acksPending() {
        return acksPending;
    }

    @Prove(complexity = Complexity.O_1, n = "", count = {})
    public void setAcksPending(boolean acksPending) {
        this.acksPending = acksPending;
    }


    @Prove(complexity = Complexity.O_1, n = "", count = {})
    public DeleteRecordsPartitionResult responseStatus() {
        return responseStatus;
    }

    @Prove(complexity = Complexity.O_1, n = "", count = {})
    public long requiredOffset() {
        return requiredOffset;
    }

    @Override
    @Prove(complexity = Complexity.O_N, n = "", count = {})
    public String toString() {
        return String.format("[acksPending: %b, error: %s, lowWatermark: %d, requiredOffset: %d]",
                acksPending, Errors.forCode(responseStatus.errorCode()).toString(), responseStatus.lowWatermark(),
                requiredOffset);
    }


}
