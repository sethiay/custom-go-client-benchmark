def download_chunks_concurrently(
    bucket_name, blob_name, filename, chunk_size=32 * 1024 * 1024, workers=8
):
  """Download a single file in chunks, concurrently in a process pool."""

  # The ID of your GCS bucket
  # bucket_name = "your-bucket-name"

  # The file to be downloaded
  # blob_name = "target-file"

  # The destination filename or path
  # filename = ""

  # The size of each chunk. The performance impact of this value depends on
  # the use case. The remote service has a minimum of 5 MiB and a maximum of
  # 5 GiB.
  # chunk_size = 32 * 1024 * 1024 (32 MiB)

  # The maximum number of processes to use for the operation. The performance
  # impact of this value depends on the use case, but smaller files usually
  # benefit from a higher number of processes. Each additional process occupies
  # some CPU and memory resources until finished. Threads can be used instead
  # of processes by passing `worker_type=transfer_manager.THREAD`.
  # workers=8

  from google.cloud.storage import Client, transfer_manager

  storage_client = Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)

  transfer_manager.download_chunks_concurrently(
      blob, filename, chunk_size=chunk_size, max_workers=workers
  )

  print("Downloaded {} to {}.".format(blob_name, filename))

import time
start = time.time()
download_chunks_concurrently("ayushsethi-parallel-downloads", "1000G/fio/Workload.0/0", "/dev/null", chunk_size=50*1024*1024, workers=96)
end = time.time()
print("Time taken: ", end - start)
