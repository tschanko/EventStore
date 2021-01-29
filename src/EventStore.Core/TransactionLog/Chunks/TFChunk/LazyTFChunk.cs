using System;

namespace EventStore.Core.TransactionLog.Chunks.TFChunk {
	public class LazyTFChunk : ITFChunk {
		private readonly TFChunk _baseChunk;
		private bool _loaded;
		private readonly Action _loadAction;
		public LazyTFChunk(TFChunk baseChunk, bool verifyHash, bool optimizeReadSideCache) {
			_baseChunk = baseChunk;
			_loadAction = () => _baseChunk.InitCompleted(verifyHash, optimizeReadSideCache);
		}
		public LazyTFChunk(TFChunk baseChunk, int writePosition, bool checkSize) {
			_baseChunk = baseChunk;
			_loadAction = () => _baseChunk.InitOngoing(writePosition, checkSize);
		}
		//todo; determine when we need to load
		public bool IsReadOnly => _baseChunk.IsReadOnly;

		public bool IsCached => _baseChunk.IsCached;

		public long LogicalDataSize => _baseChunk.LogicalDataSize;

		public int PhysicalDataSize => _baseChunk.PhysicalDataSize;

		public string FileName => _baseChunk.FileName;

		public int FileSize => _baseChunk.FileSize;

		public ChunkHeader ChunkHeader => _baseChunk.ChunkHeader;

		public ChunkFooter ChunkFooter => _baseChunk.ChunkFooter;

		public int RawWriterPosition => _baseChunk.RawWriterPosition;

		public void Load() {
			if (!_loaded) {
				try {
					_loadAction();
				} catch {
					_baseChunk.Dispose();
					throw;
				}
			}
			_loaded = true;
		}
		public void CacheInMemory() {
			_baseChunk.CacheInMemory();
		}

		public void UnCacheFromMemory() {
			_baseChunk.UnCacheFromMemory();
		}

		public bool ExistsAt(long logicalPosition) {
			return _baseChunk.ExistsAt(logicalPosition);
		}

		public void WaitForDestroy(int timeoutMs) {
			_baseChunk.WaitForDestroy(timeoutMs);
		}

		public void MarkForDeletion() {
			_baseChunk.MarkForDeletion();
		}

		public void Complete() {
			_baseChunk.Complete();
		}

		public void VerifyFileHash() {
			_baseChunk.VerifyFileHash();
		}

		public void Dispose() {
			_baseChunk.Dispose();
		}
	}
}
