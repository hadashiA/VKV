namespace VKV.Internal;

using System;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Multi procuder single consumer fixed-size lock-free queue
/// </summary>
/// <typeparam name="T"></typeparam>
sealed class MpscRingQueue<T> where T : class
{
    readonly T[] buffer;
    readonly int mask;   // 2pod
    long head;           // Update by dequeue (single consumer)
    long tail;           // Update by enqueue (multiple producer)

    public int Capacity => buffer.Length;

    public MpscRingQueue(int capacityPowerOfTwo)
    {
        if (capacityPowerOfTwo <= 0 || (capacityPowerOfTwo & (capacityPowerOfTwo - 1)) != 0)
        {
            throw new ArgumentException("capacity must be power of two", nameof(capacityPowerOfTwo));
        }

        buffer = new T[capacityPowerOfTwo];
        mask = capacityPowerOfTwo - 1;
        head = 0;
        tail = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    int Index(long seq) => (int)(seq & mask);

    public bool TryEnqueue(T item)
    {
        while (true)
        {
            var t = Volatile.Read(ref tail);
            var h = Volatile.Read(ref head);

            if (t - h >= buffer.Length)
            {
                // full
                return false;
            }

            if (Interlocked.CompareExchange(ref tail, t + 1, t) != t)
            {
                // retry
                continue;
            }

            var index = Index(t);
            buffer[index] = item; // single-writer
            return true;
        }
    }

    public bool TryDequeue(out T item)
    {
        var h = Volatile.Read(ref head);
        var t = Volatile.Read(ref tail);

        if (h >= t)
        {
            item = null!;
            return false; // empty
        }

        var index = Index(h);
        item = buffer[index];
        buffer[index] = null!;
        Volatile.Write(ref head, h + 1);
        return true;
    }
}