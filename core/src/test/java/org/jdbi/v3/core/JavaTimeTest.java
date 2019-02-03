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
package org.jdbi.v3.core;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import org.jdbi.v3.core.rule.DatabaseRule;
import org.jdbi.v3.core.rule.HsqlDatabaseRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.time.temporal.ChronoUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;

public class JavaTimeTest {
    private static final ZoneOffset ZONE_IN = ZoneOffset.UTC;
    private static final ZoneOffset ZONE_OUT = ZoneOffset.ofHours(2);

    @Rule
    public DatabaseRule db = new HsqlDatabaseRule();

    private Handle h;

    @Before
    public void before() {
        h = db.getJdbi().open();
        h.createUpdate("SET TIME ZONE INTERVAL '0:00' HOUR TO MINUTE").execute();
        TimeZone.setDefault(TimeZone.getTimeZone(ZONE_IN));
    }

    @Test
    public void instant() {
        Instant now = Instant.now();

        createTable("timestamp");
        insert(now);
        scatterTimeZones();
        assertThat(get(Instant.class)).isEqualTo(now);
    }

    @Test
    public void localDate() {
        LocalDate now = LocalDate.now();

        createTable("date");
        insert(now);
        scatterTimeZones();
        assertThat(get(LocalDate.class)).isEqualTo(now);
    }

    @Test
    public void localTime() {
        LocalTime now = LocalTime.now();

        createTable("time");
        insert(now);
        scatterTimeZones();
        assertThat(get(LocalTime.class).atOffset(ZONE_OUT).withOffsetSameInstant(ZONE_IN).toLocalTime()).isEqualTo(now.truncatedTo(SECONDS));
    }

    @Test
    public void offsetTime() {
        OffsetTime now = OffsetTime.now();

        createTable("time");
        insert(now);
        scatterTimeZones();
        assertThat(get(OffsetTime.class).withOffsetSameLocal(ZONE_IN)).isEqualTo(now.truncatedTo(SECONDS));
    }

    @Test
    public void localDateTime() {
        LocalDateTime now = LocalDateTime.now();

        createTable("timestamp");
        insert(now);
        scatterTimeZones();
        assertThat(get(LocalDateTime.class).atOffset(ZONE_OUT).withOffsetSameInstant(ZONE_IN).toLocalDateTime()).isEqualTo(now);
    }

    @Test
    public void offsetDateTime() {
        OffsetDateTime now = OffsetDateTime.now();

        createTable("timestamp with time zone");
        insert(now);
        scatterTimeZones();
        assertThat(get(OffsetDateTime.class).withOffsetSameInstant(ZONE_IN)).isEqualTo(now);
    }

    @Test
    public void zonedDateTime() {
        ZonedDateTime now = ZonedDateTime.now();

        createTable("timestamp with time zone");
        insert(now);
        scatterTimeZones();
        assertThat(get(ZonedDateTime.class)).isEqualTo(now);
    }

    @Test
    public void zoneId() {
        ZoneId zone = ZoneId.systemDefault();

        createTable("varchar(50)");
        insert(zone);
        assertThat(get(ZoneId.class)).isEqualTo(zone);
    }

    @Test
    public void zoneOffset() {
        ZoneOffset zone = ZoneOffset.ofHours(6);

        createTable("varchar(50)");
        insert(zone);
        assertThat(get(ZoneOffset.class)).isEqualTo(zone);
    }

    @Test
    public void year() {
        Year now = Year.now();

        createTable("int");
        insert(now);
        assertThat(get(Year.class)).isEqualTo(now);
    }

    @Test
    public void yearMonth() {
        YearMonth now = YearMonth.now();

        createTable("int");
        insert(now);
        assertThat(get(YearMonth.class)).isEqualTo(now);
    }

    private void createTable(String columnType) {
        h.createUpdate("create table foo (bar <type>)").define("type", columnType).execute();
    }

    private <T> void insert(T value) {
        h.createUpdate("insert into foo(bar) values(:bar)").bind("bar", value).execute();
    }

    private void scatterTimeZones() {
        h.createUpdate("SET TIME ZONE INTERVAL '-1:00' HOUR TO MINUTE").execute();
        TimeZone.setDefault(TimeZone.getTimeZone(ZONE_OUT));
    }

    private <T> T get(Class<T> clazz) {
        return h.createQuery("select bar from foo").mapTo(clazz).findOnly();
    }
}
