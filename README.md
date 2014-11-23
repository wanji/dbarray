Array stored in database
========================

`DBArray` is an 2D array stored in database.
The purpose of this class is to provide a way to store and access large
array which can not be loaded into memory.

## Example

```python
import numpy as np
from dbarray import DBArray

# Create random array
nrows = 5
ncols = 8
dtype = np.float32
arr = np.random.random((nrows, ncols))
arr = np.require(arr, dtype)
print arr

""" `DBArray` from scratch
"""
# Create `DBArray`
dba1 = DBArray('test1.db')
# Initialize
dba1.set_shape((nrows, ncols))
dba1.set_dtype(dtype)

# Set rows
for i in range(nrows):
    dba1[i] = arr[i]

# Convert to ndarray
print dba1.tondarray()

""" `DBArray` from `ndarray`
"""
# Create `DBArray`
dba2 = DBArray.fromndarray(arr, 'test2.db')

# Check attributes
print dba2.nrows, dba2.ncols, dba2.dtype

# Convert to ndarray
print dba2.tondarray()
```
