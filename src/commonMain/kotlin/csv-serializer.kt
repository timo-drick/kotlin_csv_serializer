package de.drick.csvserialization

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

/**
 * apps on air
 *
 * @author Timo Drick
 */
@OptIn(ExperimentalSerializationApi::class)
class CsvElementDecoder(private val columnIndexMap: Map<String, Int>,
                        override val serializersModule: SerializersModule) : AbstractDecoder() {
    private var elementIndex = 0
    internal var columnIndex = 0
    private var elementList = emptyList<String>()

    override fun decodeNotNullMark(): Boolean = getElement().isNotBlank()
    override fun decodeBoolean(): Boolean = getElement().toBoolean()
    override fun decodeByte(): Byte = getElement().toByte()
    override fun decodeShort(): Short = getElement().toShort()
    override fun decodeInt(): Int = getElement().toInt()
    override fun decodeLong(): Long = getElement().toLong()
    override fun decodeFloat(): Float = getElement().toFloat()
    override fun decodeDouble(): Double = getElement().toDouble()
    override fun decodeString(): String = getElement()
    override fun decodeChar(): Char = getElement().first()
    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val element = getElement()
        val index = enumDescriptor.getElementIndex(element)
        if (index < 0) throw SerializationException("Enum constant: [$element] not found!")
        return index
    }
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        if (elementIndex == descriptor.elementsCount) return CompositeDecoder.DECODE_DONE
        val fieldName = descriptor.getElementName(elementIndex).toLowerCase()
        columnIndex = columnIndexMap[fieldName] ?: throw error("No header found for field: $fieldName")
        return elementIndex++
    }

    private fun getElement(): String = elementList[columnIndex]

    fun <T> decodeLine(nexLine: String, deserializer: DeserializationStrategy<T>): T {
        elementIndex = 0
        elementList = parseCsvLine(nexLine)
        return decodeSerializableValue(deserializer)
    }
}

const val UTF8_BOM = '\uFEFF'
private fun parseCsvLine(line: String, separator: Char = ','): List<String> {
    val result = mutableListOf<String>()
    val builder = StringBuilder()
    var quotes = 0
    for (ch in line.trimStart(UTF8_BOM)) {
        when {
            ch == '\"' -> {
                quotes++
            }
            (ch == '\n') || (ch == '\r') -> {
            }
            (ch == separator) && (quotes % 2 == 0) -> {
                result.add(builder.toString())
                builder.setLength(0)
            }
            else -> builder.append(ch)
        }
    }
    result.add(builder.toString())
    return result
}

@OptIn(ExperimentalSerializationApi::class)
class CsvEncoder(override val serializersModule: SerializersModule) : AbstractEncoder() {
    private val line = mutableListOf<String>()
    override fun encodeValue(value: Any) {
        line.add(value.toString())
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        line.add(enumDescriptor.getElementName(index))
    }

    override fun encodeNull() {
        line.add("")
    }
    fun <T> encodeHeader(serializer: SerializationStrategy<T>): String {
        line.clear()
        for (i in 0 until serializer.descriptor.elementsCount) {
            line.add(serializer.descriptor.getElementName(i))
        }
        return encodeCsvLine(line)
    }
    fun <T> encodeLine(value: T, serializer: SerializationStrategy<T>): String {
        line.clear()
        encodeSerializableValue(serializer, value)
        return encodeCsvLine(line)
    }

    private fun encodeCsvLine(columns: List<String>): String {
        return columns.joinToString(",") {
            "\"$it\""
        }
    }
}

expect fun useFile(filePath: String, lineSequence: (Sequence<String>) -> Unit)

sealed class CsvLine<out T: Any> {
    data class Item<T: Any>(val value: T): CsvLine<T>()
    data class Error(val error: CsvLineException): CsvLine<Nothing>()
}

class CsvLineException(line: Int, columnName: String, cause: Throwable) : SerializationException(
    message = """Unable to parse line: $line column: "$columnName". Error: ${cause.message}""",
    cause = cause
)

@OptIn(ExperimentalSerializationApi::class)
class Csv(private val module: SerializersModule = EmptySerializersModule) {
    inline fun <reified T: Any> decodeFileCollectErrors(filePath: String, crossinline block: (T) -> Unit): List<CsvLineException> {
        val errors = mutableListOf<CsvLineException>()
        decodeFromFile<T>(filePath) { lineIterator ->
            while (lineIterator.hasNext()) {
                val next = lineIterator.next()
                when (next) {
                    is CsvLine.Item -> block(next.value)
                    is CsvLine.Error -> errors.add(next.error)
                }
            }
        }
        return errors
    }
    inline fun <reified T: Any> decodeFromFile(filePath: String, crossinline block: (Iterator<CsvLine<T>>) -> Unit) {
        useFile(filePath) { lineSequence ->
            val iterator = decodeIterator<T>(lineSequence.iterator(), serializer())
            block(iterator)
        }
    }
    inline fun <reified T: Any> decodeFromSequence(list: Sequence<String>): Sequence<CsvLine<T>> = decodeFromSequence(list, serializer())

    fun <T: Any> decodeFromSequence(list: Sequence<String>, deserializer: DeserializationStrategy<T>)
            = decodeIterator(list.iterator(), deserializer).asSequence()

    fun <T: Any> decodeIterator(iter: Iterator<String>, deserializer: DeserializationStrategy<T>): Iterator<CsvLine<T>> = iterator {
        val iterator = iter.withIndex()
        val header = iterator.next()
        val headers = parseCsvLine(header.value)
        val nameToIndexMap: Map<String, Int> = headers
            .mapIndexed { index, key -> Pair(key.toLowerCase(), index) }
            .associate { it }
        val decoder = CsvElementDecoder(nameToIndexMap, module)
        iterator.forEach { (index, value) ->
            try {
                yield(CsvLine.Item(decoder.decodeLine(value, deserializer)))
            } catch (err: Throwable) {
                val line = index + 1
                val columnIndex = decoder.columnIndex
                val columnName = headers[columnIndex]
                yield(CsvLine.Error(CsvLineException(line, columnName, err)))
            }
        }
    }

    inline fun <reified T> encodeIterator(iter: Iterator<T>): Iterator<String> = encodeIterator(iter, serializer())
    fun <T> encodeIterator(iter: Iterator<T>, serializer: SerializationStrategy<T>): Iterator<String> = iterator {
        val encoder = CsvEncoder(module)
        yield(encoder.encodeHeader(serializer))
        iter.forEach {
            yield(encoder.encodeLine(it, serializer))
        }
    }
}
