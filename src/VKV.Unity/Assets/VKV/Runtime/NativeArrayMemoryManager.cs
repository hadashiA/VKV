using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using System;
using System.Buffers;


namespace VKV.Unity
{
    static class NativeArrayExtensions
    {
        public static unsafe Memory<T> AsMemory<T>(this NativeArray<T> nativeArray) where T : unmanaged
        {
            return new NativeArrayMemoryManager<T>(nativeArray).Memory;
        }
    }

    unsafe class NativeArrayMemoryManager<T> : MemoryManager<T> where T : unmanaged
    {
        public T* Ptr { get; }
        public int Length { get; }

        NativeArray<T> nativeArray;
        readonly bool ownesArray;

        public NativeArrayMemoryManager(NativeArray<T> nativeArray, bool ownesArray = true)
        {
            this.nativeArray = nativeArray;
            this.ownesArray = ownesArray;
            Ptr = (T*)nativeArray.GetUnsafeReadOnlyPtr();
            Length = nativeArray.Length;
        }

        public override Span<T> GetSpan() => new(Ptr, Length);

        /// <summary>
        /// Provides access to a pointer that represents the data (note: no actual pin occurs)
        /// </summary>
        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if (elementIndex < 0 || elementIndex >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(Length));
            }
            return new MemoryHandle(Ptr + elementIndex);
        }

        /// <summary>
        /// Has no effect
        /// </summary>
        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
            if (ownesArray)
            {
                nativeArray.Dispose();
            }
        }
    }
}
