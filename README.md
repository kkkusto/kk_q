flowchart TD
    subgraph InsertionProcess [Insertion Process]
        A1[Get table partitions]
        A2[Find min and max year/yr_mnth partition range from part1 table]
        A3[Add partitions based on this range to part2 table]
        A4[Send email of total rows copied with batch ID]
        A1 --> A2 --> A3 --> A4
    end

    subgraph DeletionProcess [Deletion Process]
        B1[Run deletion script with batch ID]
        B2[Get table entry for batch ID]
        B3[Delete partitions from part1 table]
        B4[Send email after delete operation]
        B1 --> B2 --> B3 --> B4
    end

    InsertionProcess --> DeletionProcess
