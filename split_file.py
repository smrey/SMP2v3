import os
from math import floor
import hashlib
import base64


class SplitFile:

    def __init__(self, file_to_split):
        self.max_chunk_size = 25000000  # maximum file chunk size for upload is 25MB
        self.to_split = file_to_split

    def file_size(self):
        return os.path.getsize(self.to_split)

    def get_file_chunk_size(self):
        file_size = os.path.getsize(self.to_split)
        #print(file_size) error checking
        file_chunks = file_size / self.max_chunk_size
        file_chunk_size = self.max_chunk_size
        final_chunk_size = file_size - (floor(file_chunks) * self.max_chunk_size)
        chunks = [file_chunk_size for i in range(floor(file_chunks))]
        chunks.append(final_chunk_size)
        #print(sum(chunks)) error checking
        return chunks

    def split_file(self, chunk_sizes):
        files_written = []
        with open(self.to_split, 'rb') as fr:
            for chunk_number, chunk in enumerate(chunk_sizes):
                file_chunk = fr.read(chunk_sizes[chunk_number])
                file_to_write = f"{self.to_split}_{chunk_number + 1}"
                with open(file_to_write, 'wb') as fw:
                    fw.write(file_chunk)
                    files_written.append(file_to_write)
        return files_written

    @staticmethod
    def calc_md5_hex(file_to_hash):
        hash = hashlib.md5()
        with open(file_to_hash, 'rb') as fr:
            hash.update(fr.read())
        return hash.hexdigest()

    @staticmethod
    def calc_md5_b64(file_to_hash):
        hash = hashlib.md5()
        with open(file_to_hash, 'rb') as fr:
            hash.update(fr.read())
        return base64.b64encode(hash.digest())

