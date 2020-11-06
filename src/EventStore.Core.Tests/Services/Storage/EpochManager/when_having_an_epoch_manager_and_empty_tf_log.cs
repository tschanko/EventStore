using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using NUnit.Framework;
using NUnit.Framework.Internal;
using EventStore.Core.Services.Storage.EpochManager;
using EventStore.Core.Tests.Helpers;


namespace EventStore.Core.Tests.Services.Storage {
	[TestFixture]
	public sealed class when_having_an_epoch_manager_and_empty_tf_log : SpecificationWithDirectoryPerTestFixture, IDisposable {
		private TFChunkDb _db;
		private EpochManager _epochManager;
		private TFChunkReader _reader;
		private TFChunkWriter _writer;
		private IBus _mainBus;
		private readonly Guid _instanceId = Guid.NewGuid();
		private readonly List<Message> _published = new List<Message>();

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_mainBus = new InMemoryBus(nameof(when_having_an_epoch_manager_and_empty_tf_log));
			_mainBus.Subscribe(new AdHocHandler<SystemMessage.EpochWritten>(m=> _published.Add(m)));
			_db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName,0));
			_db.Open();
			_reader = new TFChunkReader(_db, _db.Config.WriterCheckpoint);
			_writer = new TFChunkWriter(_db);
			
			_epochManager = new EpochManager(_mainBus,
				10,
				_db.Config.EpochCheckpoint,
				_writer,
				initialReaderCount: 1,
				maxReaderCount: 5,
				readerFactory: () => new TFChunkReader(_db, _db.Config.WriterCheckpoint,
					optimizeReadSideCache: _db.Config.OptimizeReadSideCache),
				_instanceId);
			_epochManager.Init();
		}

		[OneTimeTearDown]
		public override async Task TestFixtureTearDown() {
			this.Dispose();
			await base.TestFixtureTearDown();
		}
		
		[Test]
		public void can_write_first_epoch() {
			_published.Clear();
			var beforeWrite = DateTime.Now;
			_epochManager.WriteNewEpoch(0);
			Assert.That(_published.Count ==1);
			var epochWritten = _published[0] as SystemMessage.EpochWritten;
			Assert.NotNull(epochWritten);
			Assert.That(epochWritten.Epoch.EpochNumber == 0);
			Assert.That(epochWritten.Epoch.PrevEpochPosition == -1);
			Assert.That(epochWritten.Epoch.EpochPosition == 0);
			Assert.That(epochWritten.Epoch.LeaderInstanceId == _instanceId);
			Assert.That(epochWritten.Epoch.TimeStamp < DateTime.Now);
			Assert.That(epochWritten.Epoch.TimeStamp >= beforeWrite);
			_published.Clear();
		}

		public void Dispose() {
			//epochManager?.Dispose();
			//reader?.Dispose();
			_writer?.Dispose();
			_db?.Close();
			_db?.Dispose();
		}
	}
}
