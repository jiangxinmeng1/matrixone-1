package metadata

type MetaInfo struct {
	Key   string
	Off   uint32
	Size  uint32
	OSize uint32
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
