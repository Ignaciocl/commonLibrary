package dynamo

import (
	"github.com/guregu/dynamo"
	log "github.com/sirupsen/logrus"
)

// We will need a sorting in a future, I will code that at some point

type Dynamo[T StorableDynamo] struct {
	table dynamo.Table
}

func (d *Dynamo[T]) Put(item T) error {
	return d.table.Put(item).Run()
}

func (d *Dynamo[T]) Get(hash string) (T, error) {
	var t T
	err := d.table.Get(t.GetTableId(), hash).One(&t)
	return t, err
}

func (d *Dynamo[T]) Scan(query string, valuesToFilter []interface{}) ([]T, error) {
	results := make([]T, 0)
	err := d.table.Scan().Filter(query, valuesToFilter).All(&results)
	return results, err
}

func (d *Dynamo[T]) QueryBy(parameter string, queryBy interface{}, filter string, valuesToFilter []interface{}) ([]T, error) {
	query := d.table.Get(parameter, queryBy)
	if len(valuesToFilter) == 0 && filter == "" {
		query.Filter(filter, valuesToFilter...)
	}
	result := make([]T, 0)
	err := query.All(&result)
	return result, err
}

func (d *Dynamo[T]) Delete(hash string) (T, error) {
	var t T
	err := d.table.Delete(t.GetTableId(), hash).OldValue(&t)
	return t, err
}

func CreateDynamoTable[T StorableDynamo](tableName string, db *dynamo.DB) (Dynamo[T], error) {
	tables := db.ListTables().Iter()
	var tableNameReceived string
	found := false
	for b := true; b; b = tables.Next(&tableNameReceived) {
		if tableName == tableNameReceived {
			found = true
			break
		}
	}
	if !found {
		var t T
		if err := db.CreateTable(tableName, t).OnDemand(true).Run(); err != nil {
			log.Errorf("could not create table: %s", err.Error())
			return Dynamo[T]{}, err
		}
	}
	table := db.Table(tableName)
	return Dynamo[T]{
		table: table,
	}, nil
}
