using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.DataStructures;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.LogRecords;
using ILogger = Serilog.ILogger;

namespace EventStore.Core.Services.Storage.EpochManager {
	public class EpochManager : IEpochManager {
		private static readonly ILogger Log = Serilog.Log.ForContext<EpochManager>();
		private readonly IPublisher _bus;

		private readonly ICheckpoint _checkpoint;
		private readonly ObjectPool<ITransactionFileReader> _readers;
		private readonly ITransactionFileWriter _writer;
		private readonly Guid _instanceId;

		private readonly object _locker = new object();
		private readonly int _cacheSize;
		private readonly LinkedList<EpochRecord> _epochs = new LinkedList<EpochRecord>();

		private LinkedListNode<EpochRecord> _firstCachedEpoch;
		private LinkedListNode<EpochRecord> _lastCachedEpoch;
		public EpochRecord GetLastEpoch() => _lastCachedEpoch?.Value;
		public int LastEpochNumber => _lastCachedEpoch?.Value.EpochNumber ?? -1;
		private int FirstCachedEpochNumber => _firstCachedEpoch?.Value.EpochNumber ?? -1;

		public EpochManager(IPublisher bus,
			int cachedEpochCount,
			ICheckpoint checkpoint,
			ITransactionFileWriter writer,
			int initialReaderCount,
			int maxReaderCount,
			Func<ITransactionFileReader> readerFactory,
			Guid instanceId) {
			Ensure.NotNull(bus, "bus");
			Ensure.Nonnegative(cachedEpochCount, "cachedEpochCount");
			Ensure.NotNull(checkpoint, "checkpoint");
			Ensure.NotNull(writer, "chunkWriter");
			Ensure.Nonnegative(initialReaderCount, "initialReaderCount");
			Ensure.Positive(maxReaderCount, "maxReaderCount");
			if (initialReaderCount > maxReaderCount)
				throw new ArgumentOutOfRangeException(nameof(initialReaderCount),
					"initialReaderCount is greater than maxReaderCount.");
			Ensure.NotNull(readerFactory, "readerFactory");

			_bus = bus;
			_cacheSize = cachedEpochCount;
			_checkpoint = checkpoint;
			_readers = new ObjectPool<ITransactionFileReader>("EpochManager readers pool", initialReaderCount,
				maxReaderCount, readerFactory);
			_writer = writer;
			_instanceId = instanceId;
		}

		public void Init() {
			ReadEpochs(_cacheSize);
		}

		private void ReadEpochs(int maxEpochCount) {
			lock (_locker) {
				var reader = _readers.Get();
				try {

					long epochPos = _checkpoint.Read();
					if (epochPos < 0) // we probably have lost/uninitialized epoch checkpoint scan back to find the most recent epoch in the log
					{
						reader.Reposition(_writer.Checkpoint.Read());

						SeqReadResult result;
						while ((result = reader.TryReadPrev()).Success) {
							var rec = result.LogRecord;
							if (rec.RecordType != LogRecordType.System ||
								((SystemLogRecord)rec).SystemRecordType != SystemRecordType.Epoch)
								continue;
							epochPos = rec.LogPosition;
							break;
						}
					}

					//read back down the chain of epochs in the log until the cache is full
					int cnt = 0;
					while (epochPos >= 0 && cnt < maxEpochCount) {
						var result = reader.TryReadAt(epochPos);
						if (!result.Success)
							throw new Exception($"Could not find Epoch record at LogPosition {epochPos}.");
						if (result.LogRecord.RecordType != LogRecordType.System)
							throw new Exception($"LogRecord is not SystemLogRecord: {result.LogRecord}.");

						var sysRec = (SystemLogRecord)result.LogRecord;
						if (sysRec.SystemRecordType != SystemRecordType.Epoch)
							throw new Exception($"SystemLogRecord is not of Epoch sub-type: {result.LogRecord}.");

						var epoch = sysRec.GetEpochRecord();
						_epochs.AddFirst(epoch);
						epochPos = epoch.PrevEpochPosition;
						cnt += 1;
					}
					_lastCachedEpoch = _epochs.Last;
					_firstCachedEpoch = _epochs.First;
				} finally {
					_readers.Return(reader);
				}
			}
		}

		public EpochRecord[] GetLastEpochs(int maxCount) {
			lock (_locker) {
				var res = new List<EpochRecord>();
				var node = _epochs.Last;
				while (node != null & res.Count < maxCount) {
					res.Add(node.Value);
					node = node.Previous;
				}
				return res.ToArray();
			}
		}

		public EpochRecord GetEpochAfter(int epochNumber, bool throwIfNotFound) {
			if (epochNumber == LastEpochNumber) {
				if (!throwIfNotFound)
					return null;
				throw new ArgumentOutOfRangeException(
					nameof(epochNumber),
					$"EpochNumber no epoch exist after Last Epoch {epochNumber}.");
			}

			EpochRecord epoch;
			lock (_locker) {
				var epochNode = _epochs.Last;
				while (epochNode != null && epochNode.Value.EpochNumber != epochNumber) {
					epochNode = epochNode.Previous;
				}

				epoch = epochNode?.Next?.Value;
			}

			if (epoch == null && _firstCachedEpoch?.Value != null && _firstCachedEpoch.Value.PrevEpochPosition != -1) {
				var reader = _readers.Get();
				try {
					epoch = _firstCachedEpoch.Value;
					do {
						var result = reader.TryReadAt(epoch.PrevEpochPosition);
						if (!result.Success)
							throw new Exception($"Could not find Epoch record at LogPosition {epoch.PrevEpochPosition}.");
						if (result.LogRecord.RecordType != LogRecordType.System)
							throw new Exception($"LogRecord is not SystemLogRecord: {result.LogRecord}.");

						var sysRec = (SystemLogRecord)result.LogRecord;
						if (sysRec.SystemRecordType != SystemRecordType.Epoch)
							throw new Exception($"SystemLogRecord is not of Epoch sub-type: {result.LogRecord}.");

						var nextEpoch = sysRec.GetEpochRecord();
						if(nextEpoch.EpochNumber == epochNumber){ break;} //got it

						epoch = nextEpoch;

					} while (epoch.PrevEpochPosition != -1 && epoch.EpochNumber < epochNumber);

				} finally {
					_readers.Return(reader);
				}
			}

			if (epoch == null && throwIfNotFound) { throw new Exception($"Concurrency failure, epoch #{epochNumber} should not be null."); }

			return epoch;
		}

		public bool IsCorrectEpochAt(long epochPosition, int epochNumber, Guid epochId) {
			Ensure.Nonnegative(epochPosition, "logPosition");
			Ensure.Nonnegative(epochNumber, "epochNumber");
			Ensure.NotEmptyGuid(epochId, "epochId");


			if (epochNumber > LastEpochNumber)
				return false;

			EpochRecord epoch;
			lock (_locker) {
				epoch = _epochs.FirstOrDefault(e => e.EpochNumber == epochNumber);
				if (epoch != null) {
					return epoch.EpochId == epochId && epoch.EpochPosition == epochPosition;
				}
			}

			// epochNumber < _minCachedEpochNumber
			var reader = _readers.Get();
			try {
				var res = reader.TryReadAt(epochPosition);
				if (!res.Success || res.LogRecord.RecordType != LogRecordType.System)
					return false;
				var sysRec = (SystemLogRecord)res.LogRecord;
				if (sysRec.SystemRecordType != SystemRecordType.Epoch)
					return false;

				epoch = sysRec.GetEpochRecord();
				return epoch.EpochNumber == epochNumber && epoch.EpochId == epochId;
			} finally {
				_readers.Return(reader);
			}
		}

		// This method should be called from single thread.
		public void WriteNewEpoch(int epochNumber) {
			// Now we write epoch record (with possible retry, if we are at the end of chunk) 
			// and update EpochManager's state, by adjusting cache of records, epoch count and un-caching 
			// excessive record, if present.
			// If we are writing the very first epoch, last position will be -1.
			if (epochNumber < 0) {
				throw new ArgumentException($"Cannot add write Epoch with a negative Epoch Number {epochNumber}.",nameof(epochNumber));
			}
			if (epochNumber <= LastEpochNumber) {
				throw new Exception($"Concurrency failure, Last Epoch Number is {LastEpochNumber} cannot add write new Epoch with a lower Epoch Number {epochNumber}.");
			}

			var epoch = WriteEpochRecordWithRetry(epochNumber, Guid.NewGuid(), LastEpochNumber, _instanceId);
			UpdateLastEpoch(epoch, flushWriter: true);
		}

		private EpochRecord WriteEpochRecordWithRetry(int epochNumber, Guid epochId, long lastEpochPosition, Guid instanceId) {
			long pos = _writer.Checkpoint.ReadNonFlushed();
			var epoch = new EpochRecord(pos, epochNumber, epochId, lastEpochPosition, DateTime.UtcNow, instanceId);
			var rec = new SystemLogRecord(epoch.EpochPosition, epoch.TimeStamp, SystemRecordType.Epoch,
				SystemRecordSerialization.Json, epoch.AsSerialized());

			if (!_writer.Write(rec, out pos)) {
				epoch = new EpochRecord(pos, epochNumber, epochId, lastEpochPosition, DateTime.UtcNow, instanceId);
				rec = new SystemLogRecord(epoch.EpochPosition, epoch.TimeStamp, SystemRecordType.Epoch,
					SystemRecordSerialization.Json, epoch.AsSerialized());
				if (!_writer.Write(rec, out pos))
					throw new Exception($"Second write try failed at {epoch.EpochPosition}.");
			}

			Log.Debug("=== Writing E{epochNumber}@{epochPosition}:{epochId:B} (previous epoch at {lastEpochPosition}). L={leaderId:B}.",
				epochNumber, epoch.EpochPosition, epochId, lastEpochPosition, epoch.LeaderInstanceId);

			_bus.Publish(new SystemMessage.EpochWritten(epoch));
			return epoch;
		}

		public void SetLastEpoch(EpochRecord epoch) {
			Ensure.NotNull(epoch, "epoch");
			//if in follower/clone mode we should make sure the replicated epoch is in the manager cache
			//this method is idempotent and safe to be called on the leader as well

			if (epoch.EpochPosition > LastEpochNumber) {
				UpdateLastEpoch(epoch, flushWriter: false);
				return;
			}

			// Epoch record must have been already written, so we need to make sure it is where we expect it to be.
			// If this check fails, then there is something very wrong with epochs, data corruption is possible.
			if (!IsCorrectEpochAt(epoch.EpochPosition, epoch.EpochNumber, epoch.EpochId)) {
				throw new Exception(
					$"Not found epoch at {epoch.EpochPosition} with epoch number: {epoch.EpochNumber} and epoch ID: {epoch.EpochId}. " +
					"SetLastEpoch FAILED! Data corruption risk!");
			}
		}

		private void UpdateLastEpoch(EpochRecord epoch, bool flushWriter) {
			if (LastEpochNumber == epoch.EpochNumber) {
				return;
			}
			if (LastEpochNumber > epoch.EpochNumber) {
				throw new Exception($"Concurrency failure, Last Epoch Number is {LastEpochNumber} cannot add new Last Epoch with a lower Epoch Number {epoch.EpochNumber}.");
			}

			lock (_locker) {
				if (!_epochs.Contains(epoch)) {
					_epochs.AddLast(epoch);
					_lastCachedEpoch = _epochs.Last;
					_epochs.RemoveFirst();
					_firstCachedEpoch = _epochs.First;

					if (flushWriter)
						_writer.Flush();
					// Now update epoch checkpoint, so on restart we don't scan sequentially TF.
					_checkpoint.Write(epoch.EpochPosition);
					_checkpoint.Flush();
				}
			}
			Log.Debug(
				"=== Update Last Epoch E{epochNumber}@{epochPosition}:{epochId:B} (previous epoch at {lastEpochPosition}) L={leaderId:B}.",
				epoch.EpochNumber, epoch.EpochPosition, epoch.EpochId, epoch.PrevEpochPosition, epoch.LeaderInstanceId);

		}
	}
}
