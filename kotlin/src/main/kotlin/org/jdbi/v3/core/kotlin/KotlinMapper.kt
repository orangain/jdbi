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

import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.mapper.Nested
import org.jdbi.v3.core.mapper.PropagateNull
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.mapper.SingleColumnMapper
import org.jdbi.v3.core.mapper.reflect.ColumnName
import org.jdbi.v3.core.mapper.reflect.ColumnNameMatcher
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor
import org.jdbi.v3.core.mapper.reflect.ReflectionMapperUtil.anyColumnsStartWithPrefix
import org.jdbi.v3.core.mapper.reflect.ReflectionMapperUtil.findColumnIndex
import org.jdbi.v3.core.mapper.reflect.ReflectionMapperUtil.getColumnNames
import org.jdbi.v3.core.mapper.reflect.ReflectionMappers
import org.jdbi.v3.core.mapper.reflect.internal.PojoMapper
import org.jdbi.v3.core.qualifier.QualifiedType
import org.jdbi.v3.core.statement.StatementContext
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.util.Optional
import java.util.OptionalInt
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure

private val nullValueRowMapper = RowMapper<Any?> { _, _ -> null }

class KotlinMapper(clazz: Class<*>, private val prefix: String = "") : RowMapper<Any> {
    companion object {
        private val logger = LoggerFactory.getLogger(KotlinMapper::class.java)
    }
    private val kClass: KClass<*> = clazz.kotlin
    private val constructor = findConstructor(kClass)
    private val constructorParameters = constructor.parameters
    private val memberProperties = kClass.memberProperties
        .mapNotNull { it as? KMutableProperty1<*, *> }
        .filter { property ->
            !constructorParameters.any { parameter -> parameter.paramName() == property.propName() }
        }

    private val nestedMappers = ConcurrentHashMap<KParameter, KotlinMapper>()
    private val nestedPropertyMappers = ConcurrentHashMap<KMutableProperty1<*, *>, KotlinMapper>()

    override fun map(rs: ResultSet, ctx: StatementContext): Any? {
        return specialize(rs, ctx).map(rs, ctx)
    }

    override fun specialize(rs: ResultSet, ctx: StatementContext): RowMapper<Any?> {
        val columnNames = getColumnNames(rs)
        val isColumnNullables = getIsColumnNullables(rs)
        val columnNameMatchers = ctx.getConfig(ReflectionMappers::class.java).columnNameMatchers
        val unmatchedColumns = columnNames.toMutableSet()

        val mapper = specialize0(ctx, columnNames, columnNameMatchers, unmatchedColumns, isColumnNullables)
            .orElseThrow {
                IllegalArgumentException(
                    "Mapping Kotlin type ${kClass.simpleName} didn't find any columns matching required, " +
                        "non-default constructor parameters in result set")
            }

        if (ctx.getConfig(ReflectionMappers::class.java).isStrictMatching &&
            unmatchedColumns.any { col -> col.startsWith(prefix) }) {

            throw IllegalArgumentException(
                "Mapping constructor-injected type ${kClass.simpleName} could not match parameters " +
                    "for columns: $unmatchedColumns")
        }

        return mapper
    }

    private fun specialize0(ctx: StatementContext,
                            columnNames: List<String>,
                            columnNameMatchers: List<ColumnNameMatcher>,
                            unmatchedColumns: MutableSet<String>,
                            isColumnNullables: List<Boolean>
    ): Optional<RowMapper<Any?>> {
        val resolvedConstructorParameters = constructorParameters
            .map { parameter ->
                parameter to resolveConstructorParameterMapper(
                        ctx, parameter, columnNames, columnNameMatchers, unmatchedColumns, isColumnNullables)
            }
            .sortedBy { it -> if (it.second.propagateNull) 0 else 1 }
            .associate { it -> it }

        val explicitlyMappedConstructorParameters = resolvedConstructorParameters
            .filter { it.value.type == ParamResolution.MAPPED }
            .keys
        val unmappedConstructorParameters = resolvedConstructorParameters
            .filter { it.value.type == ParamResolution.UNMAPPED }
            .keys
        if (unmappedConstructorParameters.isNotEmpty()) {
            if (explicitlyMappedConstructorParameters.isEmpty()) {
                // at least one constructor parameter is unmapped, and the rest are defaulted or nullable
                return Optional.empty()
            }
            // some constructor parameters explicitly mapped, and some unmapped
            throw IllegalArgumentException(
                "Mapping constructor-injected type ${kClass.simpleName} matched columns " +
                    "for constructor parameters ${explicitlyMappedConstructorParameters}, " +
                    "but not for ${unmappedConstructorParameters}"
            )
        }

        if (explicitlyMappedConstructorParameters.isEmpty() && memberProperties.isEmpty()) {
            // no constructor parameters or properties are mapped. nothing for us to do
            return Optional.empty()
        }

        val constructorParametersWithMappers = resolvedConstructorParameters
            // We filter 'null' mappers to remove parameters with no mappers but a default value
            .filterValues { it.mapper != null }
        val propertiesWithMappers = memberProperties
            .associateWith { property ->
                resolveMemberPropertyMapper(ctx, property, columnNames, columnNameMatchers, unmatchedColumns, isColumnNullables)
            }
            .filterValues { it.mapper != null }

        warnNullableMappings(constructorParametersWithMappers, propertiesWithMappers)

        val nullMarkerColumn =
            Optional.ofNullable(kClass.findAnnotation<PropagateNull>())
                .map(PropagateNull::value);

        return Optional.of(RowMapper mapped@{ r, c ->
            if (PojoMapper.propagateNull(r, nullMarkerColumn)) {
                return@mapped null
            }

            val constructorParametersWithValues = constructorParametersWithMappers
                .mapValues {
                    val v = it.value.mapper?.map(r, c)
                    if (v == null && it.value.propagateNull) {
                        return@mapped null
                    }
                    v
                }
                .filterValues { it != ParamResolution.USE_DEFAULT }

            val memberPropertiesWithValues = propertiesWithMappers
                .mapValues { it ->
                    val v = it.value.mapper?.map(r, c)
                    if (v == null && it.value.propagateNull) {
                        return@mapped null
                    }
                    v
                }
            constructor.isAccessible = true
            constructor.callBy(constructorParametersWithValues).also { instance ->
                memberPropertiesWithValues.forEach { (prop, value) ->
                    prop.isAccessible = true
                    prop.setter.call(instance, value)
                }
            }
        })
    }

    private fun resolveConstructorParameterMapper(ctx: StatementContext,
                                                  parameter: KParameter,
                                                  columnNames: List<String>,
                                                  columnNameMatchers: List<ColumnNameMatcher>,
                                                  unmatchedColumns: MutableSet<String>,
                                                  isColumnNullables: List<Boolean>
    ): ParamData {
        val parameterName = parameter.paramName()

        val nested = parameter.findAnnotation<Nested>()
        val propagateNull = parameter.findAnnotation<PropagateNull>() != null
        if (nested == null) {
            val columnIndex = findColumnIndex(parameterName, columnNames, columnNameMatchers) { parameter.name }
            if (columnIndex.isPresent) {
                val type = QualifiedType.of(parameter.type.javaType)
                    .withAnnotations(getQualifiers(parameter))

                return ctx.findColumnMapperFor(type)
                    .map { mapper ->
                        val m = if (parameter.isOptional && !parameter.type.isMarkedNullable) {
                            kotlinDefaultMapper(mapper)
                        } else {
                            mapper
                        }
                        ParamData(
                            ParamResolution.MAPPED,
                            SingleColumnMapper(m, columnIndex.asInt + 1),
                            propagateNull,
                            columnNames[columnIndex.asInt],
                            isColumnNullables[columnIndex.asInt])
                    }
                    .orElseThrow {
                        IllegalArgumentException(
                            "Could not find column mapper for type '$type' of parameter " +
                                "'$parameter' for constructor '$constructor'")
                    }.also {
                        unmatchedColumns.remove(columnNames[columnIndex.asInt])
                    }
            }
        } else {
            val nestedPrefix = prefix + nested.value

            if (anyColumnsStartWithPrefix(columnNames, nestedPrefix, columnNameMatchers)) {
                val nestedMapper = nestedMappers
                    .computeIfAbsent(parameter) { p ->
                        KotlinMapper(p.type.jvmErasure.java, nestedPrefix)
                    }
                    .specialize0(ctx, columnNames, columnNameMatchers, unmatchedColumns, isColumnNullables)
                if (nestedMapper.isPresent) {
                    return ParamData(ParamResolution.MAPPED, nestedMapper.get(), propagateNull)
                }
            }
        }

        if (parameter.isOptional) {
            // Parameter has no matching columns but has a default value, use the default value
            return ParamData(ParamResolution.USE_DEFAULT, null, propagateNull)
        }

        if (parameter.type.isMarkedNullable) {
            return ParamData(ParamResolution.USE_NULL, nullValueRowMapper, propagateNull)
        }

        return ParamData(ParamResolution.UNMAPPED, null, propagateNull)
    }

    private fun resolveMemberPropertyMapper(ctx: StatementContext,
                                            property: KMutableProperty1<*, *>,
                                            columnNames: List<String>,
                                            columnNameMatchers: List<ColumnNameMatcher>,
                                            unmatchedColumns: MutableSet<String>,
                                            isColumnNullables: List<Boolean>
    ): ParamData {
        val propertyName = property.propName()
        val nested = property.javaField?.getAnnotation(Nested::class.java)
        val propagateNull = property.javaField?.getAnnotation(PropagateNull::class.java) != null

        if (nested == null) {
            val possibleColumnIndex : OptionalInt = findColumnIndex(propertyName, columnNames, columnNameMatchers, { property.name })
            val columnIndex : Int = when {
                possibleColumnIndex.isPresent -> possibleColumnIndex.asInt
                ! property.isLateinit -> return ParamData(ParamResolution.UNMAPPED, null, propagateNull)
                else -> throw IllegalArgumentException(
                    "Member '${property.name}' of class '${kClass.simpleName} has no column in the result set but is lateinit. " +
                        "Verify that your result set has the columns expected, or annotate the " +
                        "property explicitly with @ColumnName"
                )
            }

            val type = property.returnType.javaType
            return ctx.findColumnMapperFor(type)
                    .map { mapper -> ParamData(
                        ParamResolution.MAPPED,
                        SingleColumnMapper(mapper, columnIndex + 1),
                        propagateNull,
                        columnNames[columnIndex],
                        isColumnNullables[columnIndex])
                    }
                    .orElseThrow {
                        IllegalArgumentException(
                            "Could not find column mapper for type '$type' of property " +
                                "'${property.name}' for constructor '${kClass.simpleName}'")
                    }
                    .also {
                        unmatchedColumns.remove(columnNames[columnIndex])
                    }
        } else {
            val nestedPrefix = prefix + nested.value

            if (anyColumnsStartWithPrefix(columnNames, nestedPrefix, columnNameMatchers)) {
                return ParamData(
                    ParamResolution.MAPPED,
                    nestedPropertyMappers
                        .computeIfAbsent(property) { p -> KotlinMapper(p.returnType.jvmErasure.java, nestedPrefix) }
                        .specialize0(ctx, columnNames, columnNameMatchers, unmatchedColumns, isColumnNullables)
                        .orElse(null),
                    propagateNull)
            }
        }

        return ParamData(ParamResolution.UNMAPPED, null, propagateNull)
    }

    private fun warnNullableMappings(constructorParametersWithMappers: Map<KParameter, ParamData>,
                                     propertiesWithMappers: Map<KMutableProperty1<*, *>, ParamData>) {
        constructorParametersWithMappers.forEach { (param, paramData) ->
            if (!paramData.propagateNull && paramData.isColumnNullable && !param.isOptional && !param.type.isMarkedNullable) {
                logger.warn("Nullable column '${paramData.columnName}' is mapped to the non-nullable " +
                    "constructor parameter without default value '${param.name}' for constructor " +
                    "'${kClass.simpleName}'. This may cause runtime null pointer exception.")
            }
        }
        propertiesWithMappers.forEach { (prop, paramData) ->
            if (!paramData.propagateNull && paramData.isColumnNullable && !prop.returnType.isMarkedNullable) {
                logger.warn("Nullable column '${paramData.columnName}' is mapped to the non-nullable property " +
                    "'${prop.name}' of class '${kClass.simpleName}'. This may cause runtime null pointer exception.")
            }
        }
    }

    private fun KParameter.paramName(): String? {
        return prefix + (findAnnotation<ColumnName>()?.value ?: name)
    }

    private fun KMutableProperty1<*, *>.propName(): String {
        val annotation = this.javaField?.getAnnotation(ColumnName::class.java)
        return prefix + (annotation?.value ?: name)
    }

    private enum class ParamResolution {
        MAPPED,
        USE_DEFAULT,
        USE_NULL,
        UNMAPPED
    }

    private data class ParamData(
        val type: ParamResolution,
        val mapper: RowMapper<*>?,
        val propagateNull: Boolean,
        val columnName: String = "",
        val isColumnNullable: Boolean = false
    )

    private fun kotlinDefaultMapper(mapper: ColumnMapper<*>): ColumnMapper<*> =
        ColumnMapper { r, columnNumber, ctx -> mapper.map(r, columnNumber, ctx) ?: ParamResolution.USE_DEFAULT }
}

private fun <C : Any> findConstructor(kClass: KClass<C>) : KFunction<C> {
    val annotatedConstructors = kClass.constructors.filter { it.findAnnotation<JdbiConstructor>() != null }
    return when {
        annotatedConstructors.isEmpty() -> kClass.primaryConstructor ?: findSecondaryConstructor(kClass)
        annotatedConstructors.size == 1 -> annotatedConstructors.first()
        else -> throw IllegalArgumentException("A bean, ${kClass.simpleName} was mapped which was not instantiable (multiple constructors marked with ${JdbiConstructor::class.simpleName})")
    }
}

private fun <C : Any> findSecondaryConstructor(kClass: KClass<C>): KFunction<C> {
    if (kClass.constructors.size == 1) {
        return kClass.constructors.first()
    } else {
        throw IllegalArgumentException("A bean, ${kClass.simpleName} was mapped which was not instantiable (cannot find appropriate constructor)")
    }
}

private fun getIsColumnNullables(rs: ResultSet): List<Boolean> {
    val metadata = rs.metaData
    val count = metadata.columnCount
    return (1..count).map { metadata.isNullable(it) == ResultSetMetaData.columnNullable }
}
