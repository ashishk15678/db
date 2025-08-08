## Steps used to implement.

1. I first made the AES encryption using gemini , which is used for partitioning keys.The implementation can be found at [`src/hashing/mod.rs`](src/hashing/aes.rs)
2. Next is physical partitions , it is used in physical partition so there is a little different logic.Logic can be found at [`src/partition/mod.rs`](src/db/partition/mod.rs)


## Working

1. `How the data flows ?`
The data is first divided into several partitions.
While providing data it is necessary to provide the partition key so that
it gets to proper physical address.

2. 

