using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.IO.LowLevel.Unsafe;

namespace VKV.Unity
{
    public class UnityNativeAllocatorFileStorage : IStorage
    {
        public static readonly StorageFactory Factory = (stream, pageSize) =>
        {
            if (stream is FileStream fs)
            {
                return new UnityNativeAllocatorFileStorage(fs.Name, pageSize);
            }
            throw new NotSupportedException();
        };

        readonly string filePath;
        public int PageSize { get; }

        public UnityNativeAllocatorFileStorage(string filePath, int pageSize)
        {
            this.filePath = filePath;
            PageSize = pageSize;
        }

        public unsafe ValueTask<IMemoryOwner<byte>> ReadPageAsync(
            PageNumber pageNumber,
            CancellationToken cancellationToken = default)
        {
            var buffer = new NativeArray<byte>(PageSize, Allocator.Persistent, NativeArrayOptions.UninitializedMemory);

            var cmd = new ReadCommand
            {
                Offset = pageNumber.Value,
                Size = PageSize,
                Buffer = (byte*)buffer.GetUnsafePtr()
            };

            var commands = new NativeArray<ReadCommand>(1, Allocator.Persistent);
            commands[0] = cmd;

            ReadHandle handle = default;
            var completionSource = new TaskCompletionSource<IMemoryOwner<byte>>();
            try
            {
                handle = AsyncReadManager.Read(filePath, (ReadCommand*)commands.GetUnsafeReadOnlyPtr(), 1);
            }
            catch
            {
                handle.Dispose();
                UnsafeUtility.Free(cmd.Buffer, Allocator.Persistent);
                commands.Dispose();
                throw;
            }

            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                try
                {
                    handle.JobHandle.Complete();
                    if (cancellationToken.IsCancellationRequested)
                    {
                        handle.Cancel();
                        completionSource.TrySetCanceled(cancellationToken);
                        return;
                    }
                    completionSource.TrySetResult(new NativeArrayMemoryManager<byte>(buffer));
                }
                catch (Exception e)
                {
                    completionSource.TrySetException(e);
                }
                finally
                {
                    handle.Dispose();
                    commands.Dispose();
                }
            }, this);

            return new ValueTask<IMemoryOwner<byte>>(completionSource.Task);
        }

        public void Dispose()
        {
        }
    }
}
