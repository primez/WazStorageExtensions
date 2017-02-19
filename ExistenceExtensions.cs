using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace smarx.WazStorageExtensions
{
    public static class ExistenceExtensions
    {
        public static bool Exists(this CloudBlockBlob blob)
        {
            try
            {
                blob.FetchAttributes();
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }
        public static bool Exists(this CloudBlobContainer container)
        {
            try
            {
                container.FetchAttributes();
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }
    }
}