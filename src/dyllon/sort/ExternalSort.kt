package dyllon.sort

import java.io.*;
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.NoSuchElementException

interface Serializer<T> {
    fun serialize(obj: T, out: DataOutput)
    fun deserialize(inp: DataInput): T
}

class DummyOutput : DataOutput {
    var size = 0L
        private set

    fun clear() {
        size = 0
    }

    override fun writeShort(v: Int) {
        size += 2
    }

    override fun writeLong(v: Long) {
        size += 8
    }

    override fun writeDouble(v: Double) {
        size += 8
    }

    override fun writeBytes(s: String?) {
        size += s!!.length
    }

    override fun writeByte(v: Int) {
        size += 1
    }

    override fun writeFloat(v: Float) {
        size += 4
    }

    override fun write(b: Int) {
        size += 4
    }

    override fun write(b: ByteArray?) {
        size += b!!.size
    }

    override fun write(b: ByteArray?, off: Int, len: Int) {
        val toWrite = b!!
        if (toWrite.size - off < len)
            throw IndexOutOfBoundsException()
        size += len
    }

    override fun writeChars(s: String?) {
        size += s!!.length * 2
    }

    override fun writeChar(v: Int) {
        size += 2
    }

    override fun writeBoolean(v: Boolean) {
        size += 1
    }

    override fun writeUTF(s: String?) {
        val toWrite = s!!
        size += toWrite.toByteArray(StandardCharsets.UTF_8).size + 2
    }

    override fun writeInt(v: Int) {
        size += 4
    }
}

interface ClosableIterator<T> : Iterator<T>, Closeable {
}

inline fun inRange(i: Int, start: Int, end: Int) {
    if (i < start || i >= end) {
        throw IndexOutOfBoundsException("$i is not between $start and $end.")
    }
}

class ExtensibleArray<T> : AbstractList<T>() {
    override var size: Int = 0
        private set
    var arr: Array<T> = arrayOfNulls<Object>(16) as Array<T>

    override fun get(index: Int): T {
        inRange(index, 0, size)
        return arr[index]
    }

    override fun set(index: Int, element: T): T {
        inRange(index, 0, size)
        val result = arr[index]
        arr[index] = element
        return result
    }

    override fun add(element: T): Boolean {
        if (size >= arr.size) {
            val newArr = arrayOfNulls<Object>(arr.size * 3 / 2) as Array<T>
            System.arraycopy(arr, 0, newArr, 0, arr.size)
            this.arr = newArr
        }
        arr[size++] = element
        return true
    }

    override fun removeAt(index: Int): T {
        inRange(index, 0, size)
        val result = arr[index]
        System.arraycopy(arr, index + 1, arr, index, size - index - 1)
        return result
    }

    override fun clear() {
        size = 0
    }

    fun parallelSort(comp: Comparator<T>) {
        Arrays.parallelSort(arr, 0, size, comp)
    }

    fun serialSort(c: Comparator<in T>) {
        Arrays.sort(arr, 0, size, c)
    }
}

fun <T> makeClosable(it: Iterator<T>) : ClosableIterator<T> {
    return object : ClosableIterator<T> {
        override fun hasNext(): Boolean {
            return it.hasNext()
        }

        override fun next(): T {
            return it.next()
        }

        override fun close() {
        }
    }
}

class BufferAllocator(val pageSize: Int) : Closeable {
    private val dataFile: File
    private var freeListHead: Long

    init {
        freeListHead = 0
        dataFile = File.createTempFile("dyllon.sort", null)
        RandomAccessFile(dataFile, "rw").use {
            it.setLength(1) // Give an initial length of 1 to make sure 0 is always an invalid address.
        }
        dataFile.deleteOnExit()
    }

    fun allocate() : Long {
        if (freeListHead == 0L) {
            RandomAccessFile(dataFile, "rw").use {
                val result = it.length()
                it.setLength(result + pageSize)
                return result
            }
        } else {
            val result = freeListHead
            RandomAccessFile(dataFile, "r").use {
                it.seek(result)
                freeListHead = it.readLong()
            }
            return result
        }
    }

    fun deallocate(address: Long) {
        RandomAccessFile(dataFile, "rw").use {
            it.seek(address)
            it.writeLong(freeListHead)
            freeListHead = address
        }
    }

    fun readPage(address: Long, dest: ByteBuffer) {
        Files.newByteChannel(dataFile.toPath(), StandardOpenOption.READ).use {
            dest.position(0)
            it.position(address)
            var readData = 0
            while (readData < pageSize) {
                readData += it.read(dest)
            }
            dest.position(0)
        }
    }

    fun writePage(address: Long, toWrite: ByteBuffer) {
        toWrite.position(0)
        Files.newByteChannel(dataFile.toPath(), StandardOpenOption.WRITE).use {
            it.position(address)
            var wroteData = 0
            while (wroteData < pageSize) {
                wroteData += it.write(toWrite)
            }
            toWrite.position(0)
        }
    }

    fun createIndexed() : IndexedFile {
        return IndexedFile(this)
    }

    override fun close() {
        dataFile.delete()
    }
}

abstract class ByteBufferOutputStream(pageSize: Int) : OutputStream() {
    protected val buffer = ByteBuffer.allocateDirect(pageSize)

    override fun write(b: Int) {
        if (!buffer.hasRemaining()) {
            this.nextBuffer()
        }
        buffer.put(b.toByte())
    }

    override fun write(b: ByteArray?) {
        this.write(b, 0, b!!.size)
    }

    override fun write(b: ByteArray?, off: Int, len: Int) {
        var written = 0
        val nonNull = b!!
        while (written < len) {
            if (!buffer.hasRemaining()) {
                this.nextBuffer()
            }
            val toWrite = Math.min(len - written, buffer.remaining())
            buffer.put(nonNull, written, toWrite)
            written += toWrite
        }
    }

    override abstract fun flush()
    protected abstract fun nextBuffer()

    override fun close() {
        this.flush()
    }
}

class ByteBufferInputStream(private val buffer: ByteBuffer) : InputStream() {
    override fun read(): Int {
        try {
            val result = buffer.get()
            return java.lang.Byte.toUnsignedInt(result)
        } catch (e: BufferUnderflowException) {
            return -1
        }
    }

    override fun available(): Int {
        return buffer.remaining()
    }

    override fun read(b: ByteArray?): Int {
        return read(b, 0, b!!.size)
    }

    override fun read(b: ByteArray?, off: Int, len: Int): Int {
        val toRead = Math.min(len, buffer.remaining())
        buffer.get(b, off, toRead)
        return toRead
    }

    override fun readAllBytes(): ByteArray {
        val result = ByteArray(buffer.remaining())
        buffer.get(result)
        return result
    }

    override fun readNBytes(b: ByteArray?, off: Int, len: Int): Int {
        return read(b, off, len)
    }

    override fun skip(n: Long): Long {
        val toSkip = Math.min(n, buffer.remaining().toLong()).toInt()
        buffer.position(buffer.position() + toSkip)
        return toSkip.toLong()
    }

    override fun transferTo(out: OutputStream?): Long {
        val toTransfer = buffer.remaining()
        val bulkCopy = ByteArray(toTransfer)
        buffer.get(toTransfer)

        val nonNull = out!!
        nonNull.write(bulkCopy)
        return toTransfer.toLong()
    }
}

class IndexedFile(private val allocator: BufferAllocator) : Closeable {
    override fun close() {
        val readBuffer = ByteBuffer.allocateDirect(allocator.pageSize)
        var currentAddr = firstAddr
        while (currentAddr != 0L) {
            allocator.readPage(currentAddr, readBuffer)
            val next = readBuffer.getLong()
            allocator.deallocate(currentAddr)
            currentAddr = next
        }
        firstAddr = 0L
    }

    private var firstAddr = 0L

    fun read() : DataInputStream {
        val buffer = ByteBuffer.allocateDirect(allocator.pageSize)
        val sequence = object : Enumeration<InputStream> {
            private var nextAddr: Long = firstAddr
            var currentInput: ByteBufferInputStream? = null

            override fun hasMoreElements(): Boolean {
                return nextAddr != 0L
            }

            override fun nextElement(): InputStream {
                allocator.readPage(nextAddr, buffer)
                nextAddr = buffer.getLong()
                val result = ByteBufferInputStream(buffer)
                currentInput = result
                return result
            }
        }

        val result = object : SequenceInputStream(sequence) {
            override fun close() {
                if (sequence.currentInput != null)
                    sequence.currentInput!!.close()
            }
        }

        return DataInputStream(result)
    }

    fun writer() : DataOutputStream {
        if (firstAddr == 0L)
            firstAddr = allocator.allocate()
        val result = object : ByteBufferOutputStream(allocator.pageSize) {
            private var currentAddr = firstAddr

            init {
                buffer.putLong(0L)
            }

            override fun flush() {
                val currentPos = this.buffer.position()
                allocator.writePage(currentAddr, this.buffer)
                this.buffer.position(currentPos)
            }

            override fun nextBuffer() {
                val nextPage = allocator.allocate()
                this.buffer.position(0)
                this.buffer.putLong(nextPage)
                allocator.writePage(currentAddr, this.buffer)
                currentAddr = nextPage
                buffer.putLong(0L)
            }
        }

        return DataOutputStream(result)
    }
}

data class QueueItem<T>(val item: T, val source: ClosableIterator<T>)
data class FileWithSize(val file: IndexedFile, val size: Long)

val defaultPageSize = 16 * (1 shl 20)
val defaultMemory = Runtime.getRuntime().maxMemory() / 4
class SmartIterator<T>(private var it: ClosableIterator<T>, private val serial: Serializer<T>, private val pageSize: Int = defaultPageSize, private val memory: Long = defaultMemory) : ClosableIterator<T> {
    constructor(it: Iterator<T>, serial: Serializer<T>, pageSize: Int = 128 * 1024, memory: Long = defaultMemory) : this(makeClosable(it), serial, pageSize, memory) {
    }

    override fun hasNext(): Boolean {
        return it.hasNext()
    }

    override fun next(): T {
        return it.next()
    }

    override fun close() {
        it.close()
    }

    private fun closeIterator(input: FileWithSize) : ClosableIterator<T> {
        val reader = input.file.read()
        return object : ClosableIterator<T> {
            private var pos = 0L
            override fun hasNext(): Boolean {
                return pos < input.size
            }

            override fun close() {
                reader.close()
                input.file.close()
            }

            override fun next(): T {
                if (!hasNext())
                    throw NoSuchElementException()
                pos++
                val result = serial.deserialize(reader)
                return result
            }
        }
    }

    fun sort(comp: Comparator<T>) : SmartIterator<T> {
        val runs = ArrayDeque<FileWithSize>()
        val toSort = ExtensibleArray<T>()
        val dumbOutput = DummyOutput()
        val allocator = BufferAllocator(pageSize)
        while (it.hasNext()) {
            val file = allocator.createIndexed()
            val output = file.writer()
            while (it.hasNext() && dumbOutput.size < memory) {
                val toAdd = it.next()
                toSort.add(toAdd)
                serial.serialize(toAdd, dumbOutput)
                dumbOutput.writeLong(0) // Overhead for pointer.
            }

            toSort.parallelSort(comp)
            for (ele in toSort) {
                serial.serialize(ele, output)
            }
            output.close()
            runs.add(FileWithSize(file, toSort.size.toLong()))
            toSort.clear()
            dumbOutput.clear()
        }
        it.close()

        val pageCount = memory / pageSize - 2
        while (runs.size > 1) {
            val heapSorter = PriorityQueue<QueueItem<T>>(pageSize, { a, b -> comp.compare(a.item, b.item) })
            while (heapSorter.size < pageCount && runs.isNotEmpty()) {
                val run = runs.removeFirst()
                val it = this.closeIterator(run)
                if (it.hasNext()) {
                    heapSorter.add(QueueItem(it.next(), it))
                } else {
                    it.close()
                }
            }

            // Merge together all the runs.
            val file = allocator.createIndexed()
            val output = file.writer()
            var currentSize = 0L
            while (heapSorter.isNotEmpty()) {
                val toAdd = heapSorter.poll()
                val it = toAdd.source
                if (it.hasNext()) {
                    heapSorter.add(QueueItem(it.next(), it))
                } else {
                    it.close()
                }
                currentSize++
                serial.serialize(toAdd.item, output)
            }

            output.close()
            runs.add(FileWithSize(file, currentSize))
        }

        val result = this.closeIterator(runs.removeFirst())
        this.it = object : ClosableIterator<T> {
            override fun hasNext(): Boolean {
                return result.hasNext()
            }

            override fun next(): T {
                return result.next()
            }

            override fun close() {
                result.close()
                allocator.close()
            }
        }
        return this
    }
}

object LongSerializer : Serializer<Long> {
    override fun serialize(obj: Long, out: DataOutput) {
        out.writeLong(obj)
    }

    override fun deserialize(inp: DataInput): Long {
        return inp.readLong()
    }
}

object ByteSerializer : Serializer<Byte> {
    override fun serialize(obj: Byte, out: DataOutput) {
        out.writeByte(obj.toInt())
    }

    override fun deserialize(inp: DataInput): Byte {
        return inp.readByte()
    }
}

object IntSerializer : Serializer<Int> {
    override fun serialize(obj: Int, out: DataOutput) {
        out.writeInt(obj)
    }

    override fun deserialize(inp: DataInput): Int {
        return inp.readInt()
    }
}

data class LargeObject(val key: ByteArray, val data: ByteArray)

object LargeObjectSerializer : Serializer<LargeObject> {
    override fun serialize(obj: LargeObject, out: DataOutput) {
        out.write(obj.key)
        out.write(obj.data)
    }

    override fun deserialize(inp: DataInput): LargeObject {
        val key = ByteArray(10)
        val data = ByteArray(90)
        inp.readFully(key)
        inp.readFully(data)
        return LargeObject(key, data)
    }
}

fun readFile(file: File, pageSize: Int) : ClosableIterator<LargeObject> {
    val input = DataInputStream(BufferedInputStream(FileInputStream(file), pageSize))
    return object : ClosableIterator<LargeObject> {
        var current: LargeObject? = LargeObjectSerializer.deserialize(input)

        override fun hasNext(): Boolean {
            return current != null
        }

        override fun next(): LargeObject {
            val result = current!!
            try {
                current = LargeObjectSerializer.deserialize(input)
            } catch (e: EOFException) {
                current = null
                this.close()
            }
            return result
        }

        override fun close() {
            input.close()
        }
    }
}

fun main(args: Array<String>) {
    val start = System.currentTimeMillis()
    var maxSize = 108L * (1 shl 24) / 2
//    var maxSize = 3 * (1L shl 30) / 100
    val bigList = object : Iterator<LargeObject> {
            private val gen = Random(8675309)
        private var pos = 0

        override fun hasNext(): Boolean {
            return pos < maxSize
        }

        override fun next(): LargeObject {
            pos++
            val key = ByteArray(10)
            val data = ByteArray(90)
            return LargeObject(key, data)
        }
    }

    val pageSize = defaultPageSize
    SmartIterator(bigList, LargeObjectSerializer, memory = defaultMemory, pageSize = pageSize).use {
        it.sort(Comparator{ it1: LargeObject, it2: LargeObject ->
            var result = 0
            var pos = 0
            while (result == 0 && pos < 10) {
                result = java.lang.Byte.compare(it1.key[pos], it2.key[pos])
                pos++
            }

            result
        })
    }

    val seconds = (System.currentTimeMillis() - start) / 1000.0
    System.out.printf("Took %f seconds to sort.\n", seconds)
}