package dynamo

type StorableDynamo interface {
	GetTableId() string
}

type Table[T StorableDynamo] interface {
	Put(item T) error
	Get(hash string) (T, error)
	Delete(hash string) (T, error)
	QueryBy(parameter string, queryBy interface{}, indexName, filter string, valuesToFilter []interface{}) ([]T, error)
	Scan(query string, valuesToFilter []interface{}) ([]T, error)
}
