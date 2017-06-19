/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.presto.audit;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestAuditLogListener
{

    @Test
    public void testBuildAuditRecord()
            throws URISyntaxException
    {
        URI uri = new URI("http://192.168.144.47:8080/v1/query/20170521_140224_00002_gd5k3");
        QueryMetadata metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select * from airdelays_s3_csv WHERE kw = 'dragon' limit 5",
                "FINISHED",
                uri,
                Optional.empty());
        QueryStatistics statistics = new QueryStatistics(Duration.ofMillis(100),
                Duration.ofMillis(105),
                Duration.ofMillis(5),
                Optional.of(Duration.ofMillis(2)),
                Optional.of(Duration.ofMillis(3)),
                10001,
                10002,
                10003,
                14,
                true,
                "operatorSummaries 01");
        QueryContext context = new QueryContext("test-user",
                Optional.of("principal"),
                Optional.of("192.168.144.47"),
                Optional.of("StatementClient 0.167"),
                Optional.of("clientInfo"),
                Optional.of("presto-cli"),
                Optional.of("catalog"),
                Optional.of("schema"),
                new HashMap<String, String>(),
                "192.168.1.12",
                "0.167t",
                "environment");
        QueryIOMetadata ioMetadata = new QueryIOMetadata(new ArrayList<QueryInputMetadata>(),
                Optional.empty());
        Optional<QueryFailureInfo> failureInfo = null;
        Instant createTime = Instant.ofEpochSecond(1497726977L);
        Instant executionStartTime = Instant.ofEpochSecond(1497726977L);
        Instant endTime = Instant.ofEpochSecond(1497726977L);
        QueryCompletedEvent queryCompletedEvent = new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.empty(),
                createTime,
                executionStartTime,
                endTime);

        Map<String, String> requiredConfig = new HashMap<String, String>();
        requiredConfig.put("event-listener.audit-log-path", "/path/01");
        requiredConfig.put("event-listener.audit-log-filename", "filename.02.log");
        AuditLogListener listener = new AuditLogListener(requiredConfig);
        AuditRecord record = listener.buildAuditRecord(queryCompletedEvent);
        Gson obj = new GsonBuilder().disableHtmlEscaping().create();

        String expected = "{\"queryId\":\"20170606_044544_00024_nfhe3\",\"query\":\"select * from airdelays_s3_csv WHERE kw = 'dragon' limit 5\",\"uri\":\"http://192.168.144.47:8080/v1/query/20170521_140224_00002_gd5k3\",\"state\":\"FINISHED\",\"cpuTime\":0.1,\"wallTime\":0.0,\"queuedTime\":0.005,\"peakMemoryBytes\":10001,\"totalBytes\":10002,\"totalRows\":10003,\"createTime\":\"20170618010117.000\",\"executionStartTime\":\"20170618010117.000\",\"endTime\":\"20170618010117.000\",\"remoteClientAddress\":\"192.168.144.47\",\"clientUser\":\"test-user\",\"userAgent\":\"StatementClient 0.167\",\"source\":\"presto-cli\"}";
        assertEquals(obj.toJson(record), expected);
    }
}
