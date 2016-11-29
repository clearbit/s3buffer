# s3buffer

Buffer that outputs files to S3.

## Usage

```go
package main

import (
  "github.com/clearbit/s3buffer"
)

func main() {
  buffer := s3buffer.NewBuffer("prefix-name", "bucket-name")

  for {
    buffer.WriteLine("Line")
  }
}
```
