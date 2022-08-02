package metadata

type MetaInfo struct {
	Key   string
	Off   uint32
	Size  uint32
	OSize uint32//origin
}

type Snapshot struct {
	Id       uint64
	StartTS  uint64
	EndTS    uint64
	MaxLSN   uint64
	MetaInfo *MetaInfo
}

// func (ss *Snapshot) RemoteLocation(bucket string, shard int) string {
// 	return fmt.Sprintf("%s/1/%d_%d/1/%v", bucket, ss.Id, shard)
// }

//bucker ss shard 80.ss
//bucket redo shard 51_80 -- offset:Len:OLen 

//ckp merge meta of blk(col meta)

// read blk meta
// merge all metadata
// put to s3, get new addr
// 
//ss
//ss+history