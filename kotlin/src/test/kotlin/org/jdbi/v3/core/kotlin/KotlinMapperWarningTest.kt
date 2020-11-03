/**
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
package org.jdbi.v3.core.kotlin

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.Nested
import org.jdbi.v3.core.mapper.PropagateNull
import org.jdbi.v3.core.rule.H2DatabaseRule
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.junit.MockitoJUnit
import org.mockito.junit.MockitoRule
import org.slf4j.LoggerFactory


class KotlinMapperWarningTest {
    @Rule
    @JvmField
    val mockitoRule: MockitoRule = MockitoJUnit.rule()

    @Rule
    @JvmField
    val dbRule: H2DatabaseRule = H2DatabaseRule().withPlugin(KotlinPlugin())

    private lateinit var handle: Handle

    @Mock
    private lateinit var mockAppender: Appender<ILoggingEvent>

    @Captor
    private lateinit var loggingEventCaptor: ArgumentCaptor<ILoggingEvent>

    @Before
    fun setup() {
        handle = dbRule.sharedHandle
        handle.execute("CREATE TABLE the_things(id integer NOT NULL, first text NOT NULL, second text, third text, fourth text)")
        handle.execute("CREATE TABLE the_other_things(id integer NOT NULL, other text NOT NULL)")

        val logger = LoggerFactory.getLogger(KotlinMapper::class.java) as Logger
        logger.detachAndStopAllAppenders()
        logger.addAppender(mockAppender)
    }

    data class NonNullableConstructorParameter(val id: Int, val second: String)

    @Test
    fun warnNullableColumnToNonNullableConstructorParameter() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<NonNullableConstructorParameter>()
            .list()

        verify(mockAppender).doAppend(loggingEventCaptor.capture())
        assertThat(nullabilityWarnings())
            .isEqualTo(listOf(
                "[WARN] Nullable column 'second' is mapped to the non-nullable constructor parameter without default value 'second' for constructor 'NonNullableConstructorParameter'. This may cause null pointer exception if actual value is null."))
    }

    data class NonNullableProperty(val id: Int) {
        var second: String = ""
    }

    @Test
    fun warnNullableColumnToNonNullableProperty() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<NonNullableProperty>()
            .list()

        verify(mockAppender).doAppend(loggingEventCaptor.capture())
        assertThat(nullabilityWarnings())
            .isEqualTo(listOf(
                "[WARN] Nullable column 'second' is mapped to the non-nullable property 'second' of class 'NonNullableProperty'. This may cause null pointer exception if actual value is null."))
    }

    data class NonNullableConstructorParameterAlias(val id: Int, val alias: String)

    @Test
    fun warnEvenIfNullableColumnIsAliased() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second as alias from the_things")
            .mapTo<NonNullableConstructorParameterAlias>()
            .list()

        verify(mockAppender).doAppend(loggingEventCaptor.capture())
        assertThat(nullabilityWarnings())
            .isEqualTo(listOf(
                "[WARN] Nullable column 'alias' is mapped to the non-nullable constructor parameter without default value 'alias' for constructor 'NonNullableConstructorParameterAlias'. This may cause null pointer exception if actual value is null."))
    }

    data class NestedNonNullableConstructorParameter(val second: String)
    data class HavingNestedNonNullableConstructorParameter(val id: Int,
                                                           @Nested val nested: NestedNonNullableConstructorParameter)

    @Test
    fun warnNullableColumnToNestedNonNullableConstructorParameter() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<HavingNestedNonNullableConstructorParameter>()
            .list()

        verify(mockAppender).doAppend(loggingEventCaptor.capture())
        assertThat(nullabilityWarnings())
            .isEqualTo(listOf(
                "[WARN] Nullable column 'second' is mapped to the non-nullable constructor parameter without default value 'second' for constructor 'NestedNonNullableConstructorParameter'. This may cause null pointer exception if actual value is null."))
    }


    data class NestedNonNullableProperty(val first: String) {
        var second: String = ""
    }

    data class HavingNestedNonNullableProperty(val id: Int,
                                               @Nested val nested: NestedNonNullableProperty)

    @Test
    fun warnNullableColumnToNestedNonNullableProperty() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, first, second from the_things")
            .mapTo<HavingNestedNonNullableProperty>()
            .list()

        verify(mockAppender).doAppend(loggingEventCaptor.capture())
        assertThat(nullabilityWarnings())
            .isEqualTo(listOf(
                "[WARN] Nullable column 'second' is mapped to the non-nullable property 'second' of class 'NestedNonNullableProperty'. This may cause null pointer exception if actual value is null."))
    }

    data class NullableConstructorParameter(val id: Int, val second: String?)

    @Test
    fun doesNotWarnNullableColumnToNullableConstructorParameter() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<NullableConstructorParameter>()
            .list()

        verify(mockAppender, never()).doAppend(loggingEventCaptor.capture())
    }

    data class NullableProperty(val id: Int) {
        var second: String? = null
    }

    @Test
    fun doesNotWarnNullableColumnToNullableProperty() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<NullableProperty>()
            .list()

        verify(mockAppender, never()).doAppend(loggingEventCaptor.capture())
    }

    data class PropagateNullConstructorParameter(val id: Int, @PropagateNull var second: String)

    @Test
    fun doesNotWarnPropagateNullConstructorParameter() {
        // warning does not require actual data but requires selecting from actual table
        handle.select("select id, second from the_things")
            .mapTo<PropagateNullConstructorParameter>()
            .list()

        verify(mockAppender, never()).doAppend(loggingEventCaptor.capture())
    }

    private fun nullabilityWarnings() = loggingEventCaptor.allValues.map { it.toString() }.filter { it.startsWith("[WARN] Nullable") }
}
