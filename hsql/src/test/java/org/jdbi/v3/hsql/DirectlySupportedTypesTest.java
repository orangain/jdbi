/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jdbi.v3.hsql;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.UUID;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.time.temporal.ChronoUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;

public class DirectlySupportedTypesTest {
    @Rule
    public JdbiRule db = JdbiRule.hsql().withPlugins();

    private Handle h;

    @Before
    public void before() {
        h = db.getHandle();
        h.createUpdate("SET TIME ZONE INTERVAL '0:00' HOUR TO MINUTE").execute();
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    }

    @Test
    public void offsetDateTime() {
        OffsetDateTime now = OffsetDateTime.now();

        createTable("timestamp with time zone");
        insert(now);
        changeTimeZone();
        assertThat(get(OffsetDateTime.class)).isEqualTo(now);
    }

    @Test
    public void offsetDateTimeNull() {
        createTable("timestamp with time zone");
        insert(null);
        changeTimeZone();
        assertThat(get(OffsetDateTime.class)).isNull();
    }

    @Test
    public void localDateTime() {
        LocalDateTime now = LocalDateTime.now();

        createTable("timestamp");
        insert(now);
        changeTimeZone();
        assertThat(get(LocalDateTime.class)).isEqualTo(now);
    }

    @Test
    public void localDateTimeNull() {
        createTable("timestamp");
        insert(null);
        changeTimeZone();
        assertThat(get(LocalDateTime.class)).isNull();
    }

    @Test
    public void offsetTime() {
        OffsetTime now = OffsetTime.now();

        createTable("time with time zone");
        insert(now);
        changeTimeZone();
        assertThat(get(OffsetTime.class)).isEqualTo(now.truncatedTo(SECONDS));
    }

    @Test
    public void offsetTimeNull() {
        createTable("time with time zone");
        insert(null);
        changeTimeZone();
        assertThat(get(OffsetTime.class)).isNull();
    }

    @Test
    public void localTime() {
        LocalTime now = LocalTime.now();

        createTable("time");
        insert(now);
        changeTimeZone();
        assertThat(get(LocalTime.class)).isEqualTo(now.truncatedTo(SECONDS));
    }

    @Test
    public void localTimeNull() {
        createTable("time");
        insert(null);
        changeTimeZone();
        assertThat(get(LocalTime.class)).isNull();
    }

    @Test
    public void localDate() {
        LocalDate now = LocalDate.now();

        createTable("date");
        insert(now);
        changeTimeZone();
        assertThat(get(LocalDate.class)).isEqualTo(now);
    }

    @Test
    public void localDateNull() {
        createTable("date");
        insert(null);
        changeTimeZone();
        assertThat(get(LocalDate.class)).isNull();
    }

    @Test
    public void uuid() {
        UUID value = UUID.randomUUID();

        createTable("uuid");
        insert(value);
        assertThat(get(UUID.class)).isEqualTo(value);
    }

    @Test
    public void uuidNull() {
        createTable("uuid");
        insert(null);
        assertThat(get(UUID.class)).isNull();
    }

    private void createTable(String columnType) {
        h.createUpdate("create table foo (bar <type>)").define("type", columnType).execute();
    }

    private <T> void insert(T value) {
        h.createUpdate("insert into foo(bar) values(:bar)").bind("bar", value).execute();
    }

    private void changeTimeZone() {
        h.createUpdate("SET TIME ZONE INTERVAL '-1:00' HOUR TO MINUTE").execute();
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(2)));
    }

    private <T> T get(Class<T> clazz) {
        return h.createQuery("select bar from foo").mapTo(clazz).findOnly();
    }
}
