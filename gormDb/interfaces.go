package gormDb

type DynamoTable[T any] interface {
	Put(item T) error
	Get(hash string) (T, error)
	Delete(hash string) (T, error)
	GetFiltered(result T, filter map[string]string, orderBy string, limit int, offset int) (int64, error)
}
