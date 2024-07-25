from google.cloud import storage
import threading
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
  # Spawn threads that will run task of load test.
  threads = []
  for thread_num in range(NUM_WORKERS):
    threads.append(
        threading.Thread(
            target=task,
            args=(OBJECT_NAME_PREFIX.format(thread_num), NUM_TIMES_PER_WORKER)))

  startTime = time.time()
  for thread in threads:
    # Thread is kept as daemon, so that it is killed when the parent process
    # is killed.
    thread.daemon = True
    thread.start()

  for thread in threads:
    thread.join()

  print("Time taken: ", time.time() - startTime)

main()

