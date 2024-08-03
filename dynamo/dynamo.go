package dynamo

import (
	"context"
	"github.com/guregu/dynamo/v2"
	log "github.com/sirupsen/logrus"
)

// ToDo do major refactor to use context always

type Dynamo[T StorableDynamo] struct {
	table dynamo.Table
}

type FilterData struct {
	FilterQuery  string
	FilterValues []interface{}
}

type RangeData struct {
	RangeName  string
	RangeValue []interface{}
	Operator   dynamo.Operator
}

func (d *Dynamo[T]) Put(item T) error {
	return d.table.Put(item).Run(context.Background())
}

func (d *Dynamo[T]) Get(hash string) (T, error) {
	var t T
	err := d.table.Get(t.GetTableId(), hash).One(context.Background(), &t)
	return t, err
}

func (d *Dynamo[T]) Scan(query string, valuesToFilter []interface{}) ([]T, error) {
	results := make([]T, 0)
	err := d.table.Scan().Filter(query, valuesToFilter).All(context.Background(), &results)
	return results, err
}

// QueryBy used to get multiple results of a query, if error then something happened with dynamo. If found none value, then empty array is returned
// parameter: The id from which you want to start fetching, this is thought as a method which will always have a parameter and a value to search by
// queryBy: The value of previous parameter
// indexName: the name of the index to look for, if empty normal index used
// filter: a filter if necessary, this has to be a query, for example "'someValue' > ?" and the ? will be replaced for the values in filterValues
// rangeData
func (d *Dynamo[T]) QueryBy(parameter string, queryBy interface{}, indexName string, filter FilterData, rangeData *RangeData, asc *bool, limit int) ([]T, error) {
	query := d.table.Get(parameter, queryBy)
	if indexName != "" {
		query.Index(indexName)
	}
	if len(filter.FilterValues) != 0 || filter.FilterQuery != "" {
		query.Filter(filter.FilterQuery, filter.FilterValues...)
	}
	if rangeData != nil {
		query.Range(rangeData.RangeName, rangeData.Operator, rangeData.RangeValue...)
	}
	if asc != nil {
		query.Order(dynamo.Order(*asc))
	}
	if limit > 0 {
		query.Limit(limit)
	}
	result := make([]T, 0)
	err := query.All(context.Background(), &result)
	return result, err
}

func (d *Dynamo[T]) Delete(hash string) (T, error) {
	var t T
	err := d.table.Delete(t.GetTableId(), hash).OldValue(context.Background(), &t)
	return t, err
}

func CreateDynamoTable[T StorableDynamo](tableName string, db *dynamo.DB) (Dynamo[T], error) {
	tables := db.ListTables().Iter()
	var tableNameReceived string
	found := false
	for b := true; b; b = tables.Next(context.Background(), &tableNameReceived) {
		if tableName == tableNameReceived {
			found = true
			break
		}
	}
	if !found {
		var t T
		if err := db.CreateTable(tableName, t).OnDemand(true).Run(context.Background()); err != nil {
			log.Errorf("could not create table: %s", err.Error())
			return Dynamo[T]{}, err
		}
	}
	table := db.Table(tableName)
	return Dynamo[T]{
		table: table,
	}, nil
}
