
flowchart TD
    Start[Start] --> GetTable[Get table and its partitions]
    GetTable --> CheckPartitions[Check partitions one by one]
    CheckPartitions --> IsOlder{Partition older than 7 years?}
    IsOlder -->|Yes| AddToArchive[Add to list of partitions to archive]
    IsOlder -->|No| NextPartition[Next partition]
    AddToArchive --> CheckPartitions
    NextPartition -->|More Partitions?| CheckPartitions
    NextPartition -->|No More Partitions| ProcessArchiveList[Process archive list]
    
    ProcessArchiveList --> IterateList[Iterate through list of archived partitions]
    IterateList --> AddPartition[Add partition to Part2 table with additional partition]
    AddPartition --> CopySuccessful{Copy successful?}
    CopySuccessful -->|Yes| AllCopied{All partitions copied?}
    CopySuccessful -->|No| Rollback[Rollback/delete all added partitions in Part2 table]
    AllCopied -->|Yes| DeleteFromPart[Delete from Part1 table]
    AllCopied -->|No| NextArchivedPartition[Next partition in archive list]
    
    Rollback --> CheckNextTable[Check next table]
    DeleteFromPart --> CheckNextTable
    CheckNextTable --> GetTable
    NextArchivedPartition --> IterateList
