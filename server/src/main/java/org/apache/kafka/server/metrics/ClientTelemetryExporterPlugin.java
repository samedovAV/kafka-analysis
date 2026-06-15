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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.samedov.annotation.Prove;
import com.samedov.annotation.Complexity;

/**
 * Plugin to register client telemetry receivers/exporters and export metrics. This class is used by the Kafka
 * server to export client metrics to the registered receivers and exporters, supporting both the deprecated
 * {@link ClientTelemetryReceiver} and the new {@link ClientTelemetryExporter} interfaces.
 */
@SuppressWarnings({"deprecation", "overloads", "removal"})
public class ClientTelemetryExporterPlugin {

    private final List<ClientTelemetryReceiver> receivers;
    private final List<ClientTelemetryExporter> exporters;

    public ClientTelemetryExporterPlugin() {
        this.receivers = Collections.synchronizedList(new ArrayList<>());
        this.exporters = Collections.synchronizedList(new ArrayList<>());
    }

    @Prove(complexity = Complexity.O_N, n = "", count = {})
    public boolean isEmpty() {
        return receivers.isEmpty() && exporters.isEmpty();
    }

    @Prove(complexity = Complexity.O_N, n = "", count = {})
    public void add(ClientTelemetryReceiver receiver) {
        receivers.add(receiver);
    }

    @Prove(complexity = Complexity.O_N, n = "", count = {})
    public void add(ClientTelemetryExporter exporter) {
        exporters.add(exporter);
    }

    @Prove(complexity = Complexity.O_1, n = "", count = {})
    public DefaultClientTelemetryPayload getPayLoad(PushTelemetryRequest request, int maxDecompressedBytes) {
        return new DefaultClientTelemetryPayload(request, maxDecompressedBytes);
    }

    @Prove(complexity = Complexity.O_N2, n = "", count = {})
    public void exportMetrics(RequestContext context, PushTelemetryRequest request, int pushIntervalMs, int maxDecompressedBytes) {
        DefaultClientTelemetryPayload payload = getPayLoad(request, maxDecompressedBytes);

        // Export to deprecated receivers
        for (ClientTelemetryReceiver receiver : receivers) {
            receiver.exportMetrics(context, payload);
        }

        // Export to new exporters with push interval context
        if (!exporters.isEmpty()) {
            DefaultClientTelemetryContext telemetryContext = new DefaultClientTelemetryContext(pushIntervalMs, context);
            for (ClientTelemetryExporter exporter : exporters) {
                exporter.exportMetrics(telemetryContext, payload);
            }
        }
    }
}
