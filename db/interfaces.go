package db

import "database/sql"

type Storable interface {
	GetPrimaryKey() string
}

type StorableIndex interface {
	Storable
	GetSecondaryKey() string
}
type DB[T Storable] interface {
	Insert(obj T)
	Update(obj T)
	Delete(key string) T
	Get(key string) (T, error)
	GetAll() ([]T, error)
}

type WithIndex[T StorableIndex] interface {
	DB[T]
	GetBoth(key, secondary string) (T, error)
	GetSecondary(secondary string) ([]T, error)
	DeleteSpecific(key string, secondaryKey string) T
	GetPrimary(key string) ([]T, error)
}

type scanner interface {
	scanRow(r *sql.Rows, d *string) error
}
