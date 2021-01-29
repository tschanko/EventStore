using System;

namespace EventStore.Core.TransactionLog.Chunks.TFChunk {
	public interface ITFChunk : IDisposable {
		bool IsReadOnly { get; }
		bool IsCached { get; }
		long LogicalDataSize { get; }
		int PhysicalDataSize { get; }
		string FileName { get; }
		int FileSize { get; }
		ChunkHeader ChunkHeader { get; }
		ChunkFooter ChunkFooter { get; }
		int RawWriterPosition { get; }
		unsafe void CacheInMemory();
		void UnCacheFromMemory();
		bool ExistsAt(long logicalPosition);
		void WaitForDestroy(int timeoutMs);
		void MarkForDeletion();
		void Complete();
		void VerifyFileHash();
	}
}
