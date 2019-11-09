import threading
import queue
import gzip
import numpy as np
from Bio.SeqIO.QualityIO import FastqGeneralIterator

my_queue = queue.Queue()

def storeInQueue(f):
  def wrapper(*args):
    my_queue.put(f(*args))
  return wrapper


@storeInQueue
def get_fastq_metadata(fastq):
    read_metadata = {}
    # Open fastq
    with gzip.open(fastq, "rt") as fh_r1:
        fq_r1 = FastqGeneralIterator(fh_r1)
        fq_r1_array = np.array([len(fq_seq.strip()) for (fq_id, fq_seq, fq_qual) in fq_r1])
    read_metadata["len_reads"] = np.amax(fq_r1_array)
    read_metadata["num_reads"] = fq_r1_array.size
    return read_metadata



t = threading.Thread(target=get_fastq_metadata, args=(fastq))
t.start()

my_data = my_queue.get()
print(my_data)