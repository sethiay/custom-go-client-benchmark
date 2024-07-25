from google.cloud import storage
import threading
import multiprocessing
import time


"""
 Change the constants here before running the script
"""
BUCKET_NAME = "ayushsethi-parallel-downloads"
NUM_WORKERS = 96
OBJECT_NAME_PREFIX = "cp/100mb/fio/Workload.{0}/0"
NUM_TIMES_PER_WORKER = 100

storage_client = storage.Client()

bucket = storage_client.bucket(BUCKET_NAME)

def download_blob(source_blob_name, destination_file_name):
  """Downloads a blob from the bucket."""


  # Construct a client side representation of a blob.
  # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
  # any content from Google Cloud Storage. As we don't need additional data,
  # using `Bucket.blob` is preferred here.
  blob = bucket.blob(source_blob_name)
  blob.download_to_filename(destination_file_name)

def task(source_blob_name, num_times):
  for _ in range(num_times):
    download_blob(source_blob_name, "/dev/null")

def main():
  startTime = time.time()
  processes = []
  for process_id in range(NUM_WORKERS):
    process = multiprocessing.Process(
        target=task,
        args=(OBJECT_NAME_PREFIX.format(process_id), NUM_TIMES_PER_WORKER))
    processes.append(process)

  for process in processes:
    process.daemon = True
    process.start()

  for process in processes:
    process.join()

  print("Time taken: ", time.time() - startTime)

main()

