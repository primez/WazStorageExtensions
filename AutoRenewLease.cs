using System;
using System.Globalization;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace smarx.WazStorageExtensions
{
    public class AutoRenewLease : IDisposable
    {
        private readonly CloudBlockBlob _blob;
        private bool _disposed;
        private readonly string _leaseId;
        private Thread _renewalThread;

        public AutoRenewLease(CloudBlockBlob blob)
        {
            _blob = blob;
            blob.Container.CreateIfNotExists();
            try
            {
                _blob.UploadFromByteArray(new byte[0], 0, 0, AccessCondition.GenerateIfNoneMatchCondition("*"));
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != 409
                    && e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed)
                    // 412 from trying to modify a blob that's leased
                    throw;
            }

            _leaseId = blob.TryAcquireLease();

            if (!HasLease) return;

            _renewalThread = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(40));
                    blob.RenewLease(_leaseId);
                }
            });

            _renewalThread.Start();
        }

        public bool HasLease => _leaseId != null;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public static void DoOnce(CloudBlockBlob blob, Action action)
        {
            DoOnce(blob, action, TimeSpan.FromSeconds(5));
        }

        public static void DoOnce(CloudBlockBlob blob, Action action, TimeSpan pollingFrequency)
        {
            // blob.Exists has the side effect of calling blob.FetchAttributes, which populates the metadata collection
            while (!blob.Exists() || blob.Metadata["progress"] != "done")
                using (var arl = new AutoRenewLease(blob))
                {
                    if (arl.HasLease)
                    {
                        action();
                        blob.Metadata["progress"] = "done";
                        blob.SetMetadata(arl._leaseId);
                    }
                    else
                    {
                        Thread.Sleep(pollingFrequency);
                    }
                }
        }

        public static void DoEvery(CloudBlockBlob blob, TimeSpan interval, Action action)
        {
            while (true)
            {
                var lastPerformed = DateTimeOffset.MinValue;
                using (var arl = new AutoRenewLease(blob))
                {
                    if (arl.HasLease)
                    {
                        blob.FetchAttributes();
                        DateTimeOffset.TryParseExact(blob.Metadata["lastPerformed"], "R", CultureInfo.CurrentCulture,
                            DateTimeStyles.AdjustToUniversal, out lastPerformed);
                        if (DateTimeOffset.UtcNow >= lastPerformed + interval)
                        {
                            action();
                            lastPerformed = DateTimeOffset.UtcNow;
                            blob.Metadata["lastPerformed"] = lastPerformed.ToString("R");
                            blob.SetMetadata(arl._leaseId);
                        }
                    }
                }
                var timeLeft = lastPerformed + interval - DateTimeOffset.UtcNow;
                var minimum = TimeSpan.FromSeconds(5); // so we're not polling the leased blob too fast
                Thread.Sleep(
                    timeLeft > minimum
                        ? timeLeft
                        : minimum);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
                if (_renewalThread != null)
                {
                    _renewalThread.Abort();
                    _blob.ReleaseLease(_leaseId);
                    _renewalThread = null;
                }

            _disposed = true;
        }

        ~AutoRenewLease()
        {
            Dispose(false);
        }
    }
}