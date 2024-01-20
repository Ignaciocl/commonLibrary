package gormDb

//special thanks to tomi for the initial code

import (
	"errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Repository[T any] struct {
	url string
	db  *gorm.DB
}

func NewPostgresRepository[T any](url string) (Repository[T], error) {
	db, err := gorm.Open(postgres.Open(url), &gorm.Config{})
	if err != nil {
		return Repository[T]{}, err
	}

	return Repository[T]{url: url, db: db}, nil
}

func (r *Repository[T]) Init(models []interface{}) error {
	return r.db.AutoMigrate(models...)
}

func (r *Repository[T]) Put(data *T) error {

	result := r.db.Save(data)
	if result.Error != nil {
		return result.Error
	}
	return nil

}
func (r *Repository[T]) Get(id int) (T, error) {
	var data T
	result := r.db.First(&data, id)
	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return data, result.Error
	}
	return data, nil
}
func (r *Repository[T]) Delete(id int) (T, error) {
	var data T
	result := r.db.Delete(&data, id)
	if result.Error != nil {
		return data, result.Error
	}
	return data, nil
}
func (r *Repository[T]) GetFiltered(filters map[string]string, orderBy string, limit int, offset int) ([]T, int64, error) {
	var model T
	result := make([]T, 0)
	var totalCount int64
	queryCount := r.db.Model(model)
	queryCount = applyFilters(queryCount, filters)
	err := queryCount.Count(&totalCount).Error
	if err != nil {
		return result, 0, err
	}

	query := r.db.Model(result)
	query = applyFilters(query, filters)

	if orderBy != "" {
		query = query.Order(orderBy)
	}

	query = query.Limit(limit).Offset(offset)

	if err := query.Find(result).Error; err != nil {
		return result, 0, err
	}

	return result, totalCount, nil

}

// applyFilters apply filters to the query
func applyFilters(query *gorm.DB, filters map[string]string) *gorm.DB {
	for campo, valor := range filters {
		query = query.Where(campo, valor)
	}
	return query
}
