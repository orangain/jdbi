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
package org.jdbi.v3.hsql.internal;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.mapper.ColumnMapperFactory;

public class GetObjectColumnMapperFactory implements ColumnMapperFactory {
    private static final Set<Class> SUPPORTED = Stream.of(
        OffsetDateTime.class,
        LocalDateTime.class,
        OffsetTime.class,
        LocalTime.class,
        LocalDate.class,

        UUID.class
    ).collect(Collectors.toSet());

    @Override
    public Optional<ColumnMapper<?>> build(Type type, ConfigRegistry config) {
        return type instanceof Class && SUPPORTED.contains(type)
            ? Optional.of((rs, i, ctx) -> rs.getObject(i, (Class<?>) type))
            : Optional.empty();
    }
}
