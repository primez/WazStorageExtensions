using System;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace smarx.WazStorageExtensions
{
    public static class LeaseBlobExtensions
    {
        public static string TryAcquireLease(this CloudBlockBlob blob)
        {
            try { return blob.AcquireLease(); }
            catch (WebException e)
            {
                if (e.Response == null || ((HttpWebResponse)e.Response).StatusCode != HttpStatusCode.Conflict) // 409, already leased
                {
                    throw;
                }
                e.Response.Close();
                return null;
            }
        }

        public static string AcquireLease(this CloudBlockBlob blob)
        {
            return blob.AcquireLeaseAsync(TimeSpan.FromSeconds(90)).Result;
        }

        private static void DoLeaseOperation(CloudBlob blob, string leaseId, LeaseAction action)
        {
            switch (action)
            {
                case LeaseAction.Acquire:
                    blob.AcquireLeaseAsync(TimeSpan.FromSeconds(90), leaseId).Wait();
                    break;
                case LeaseAction.Renew:
                    blob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId)).Wait();
                    break;
                case LeaseAction.Release:
                    blob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId)).Wait();
                    break;
                case LeaseAction.Break:
                    blob.BreakLeaseAsync(TimeSpan.FromSeconds(90)).Wait();
                    break;
                case LeaseAction.Change:
                    blob.ChangeLeaseAsync(leaseId, AccessCondition.GenerateLeaseCondition(leaseId)).Wait();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(action), action, null);
            }

            
        }

        public static void ReleaseLease(this CloudBlob blob, string leaseId)
        {
            DoLeaseOperation(blob, leaseId, LeaseAction.Release);
        }

        public static bool TryRenewLease(this CloudBlob blob, string leaseId)
        {
            try
            {
                blob.RenewLease(leaseId);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static void RenewLease(this CloudBlob blob, string leaseId)
        {
            DoLeaseOperation(blob, leaseId, LeaseAction.Renew);
        }

        public static void BreakLease(this CloudBlob blob)
        {
            DoLeaseOperation(blob, null, LeaseAction.Break);
        }

        public static void SetMetadata(this CloudBlockBlob blob, string leaseId)
        {
            blob.SetMetadata(AccessCondition.GenerateLeaseCondition(leaseId));
        }
    }
}