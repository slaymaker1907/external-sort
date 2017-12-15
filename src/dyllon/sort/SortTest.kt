package dyllon.sort

import java.io.DataInput
import java.io.DataOutput
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.util.*

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

object BigIntSerializer : Serializer<BigInteger> {
    override fun serialize(obj: BigInteger, out: DataOutput) {
        val result = obj.toByteArray()
        out.writeInt(result.size)
        out.write(result)
    }

    override fun deserialize(inp: DataInput): BigInteger {
        val size = inp.readInt()
        val result = ByteArray(size)
        inp.readFully(result)
        return BigInteger(result)
    }
}

fun main(args: Array<String>) {
    val maxSize = 108L * (1 shl 24)
    val largeIt = object : Iterator<BigInteger> {
        private var size = 0L
        private val gen = Random()

        override fun hasNext(): Boolean {
            return size < maxSize
        }

        override fun next(): BigInteger {
            val result = BigInteger(100 * 8, gen)
            size++
            return result
        }
    }

    val startTime = Instant.now()
    SmartIterator(largeIt, BigIntSerializer, memory = defaultMemory).use {
        it.sort(Comparator.naturalOrder())
    }
    val seconds = Duration.between(startTime, Instant.now()).seconds
    println("Took $seconds seconds to sort.")
}