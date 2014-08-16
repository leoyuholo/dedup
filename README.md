# File Deduplication (CUHK CSCI4180)
File deduplication and upload to openstack cloud storage. Use Rabin fingerprint as block matching algorithm.

# Instruction
Upload:
```
upload <m> <d> <b> <x> <file_to_upload>
```
* m, the window size (in bytes), which also defines the minimum chunk size
* d, the base parameter
* b, the number of bits of the anchor mask
* x, the maximum chunk size (in bytes), which limits the size of a chunk if no anchor point is found.

Example output:
```
Report Output:
Total number of chunks: 1000
Number of unique chunks: 599
Number of bytes with deduplication: 1051460
Number of bytes without deduplication: 2622095
Deduplication ratio: 40.10%
```

Download:
```
download <file_to_download>
```

Example output:
```
Report Output:
Number of chunks downloaded: 599
Number of bytes downloaded: 1051460
Number of bytes reconstructed: 2622095
```

Delete:
```
delete <file_to_delete>
```

Example output:
```
Report Output:
Number of chunks deleted: 599
Number of bytes deleted: 1051460
```
