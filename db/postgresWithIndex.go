package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type postgresWithIndex[T StorableIndex] struct {
	postgresDb[T]
	db        *sql.DB
	tableName string
}

func (p *postgresWithIndex[T]) DeleteSpecific(key string, secondaryKey string) T {
	r, err := p.GetBoth(key, secondaryKey)
	if err != nil {
		return r
	}
	p.db.Query(fmt.Sprintf("delete from %s where id = '%s' and secondary = '%s'", p.tableName, key, secondaryKey))
	return r
}

func (p *postgresWithIndex[T]) GetAll() ([]T, error) {
	data := make([]T, 0)
	r, err := p.db.Query(fmt.Sprintf("select * from %s", p.tableName))
	if err != nil {
		return data, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	return getRows[T, *postgresWithIndex[T]](r, p)
}

func CreateDBWithIndex[T StorableIndex](table string, url string) (WithIndex[T], error) {
	db, err := sql.Open("postgres", fmt.Sprintf("%s?sslmode=disable", url))

	if err != nil {
		return nil, err
	}
	if _, err := db.Query(fmt.Sprintf("select 1 from %s", table)); err != nil {
		if _, err := db.Query(fmt.Sprintf("create table %s(id varchar(300), secondary varchar(300), data json, PRIMARY KEY (id, secondary));", table)); err != nil {
			log.Errorf("error while creating table: %v", err)
			return nil, err
		}
	}
	return &postgresWithIndex[T]{db: db, tableName: table, postgresDb: postgresDb[T]{db: db, tableName: table}}, err
}

func (p *postgresWithIndex[T]) Insert(obj T) {
	_, err := p.GetBoth(obj.GetPrimaryKey(), obj.GetSecondaryKey())
	if err == nil {
		return
	}
	jsonObject, _ := json.Marshal(obj)
	log.Debugf("data is being saved: %v\n", obj)
	r, err := p.db.Query(fmt.Sprintf("insert into %s values ('%s', '%s', '%v')", p.tableName, obj.GetPrimaryKey(), obj.GetSecondaryKey(), string(jsonObject)))
	log.Debugf("rows returned %v, error %v\n", r, err)
	log.Debugf("insert into %s values (%s, '%v')", p.tableName, obj.GetPrimaryKey(), string(jsonObject))
}

func (p *postgresWithIndex[T]) Update(obj T) {
	_, err := p.GetBoth(obj.GetPrimaryKey(), obj.GetSecondaryKey())
	if err != nil {
		return
	}
	jsonObject, _ := json.Marshal(obj)
	log.Debugf("data is being saved: %v\n", obj)
	r, err := p.db.Query(fmt.Sprintf("update %s SET data='%v' where id = '%s' and secondary ='%s'", p.tableName, string(jsonObject), obj.GetPrimaryKey(), obj.GetSecondaryKey()))
	log.Debugf("rows returned %v, error %v\n", r, err)
}

func (p *postgresWithIndex[T]) GetBoth(key string, secondary string) (T, error) {
	var id string
	var secondaryKey string
	var d string
	var data T
	r, err := p.db.Query(fmt.Sprintf("select * from %s where id = '%s' and secondary ='%s'", p.tableName, key, secondary))
	if err != nil {
		return data, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	r.Next()
	err = r.Scan(&id, &secondaryKey, &d)
	if err != nil {
		log.Errorf("error while getting with %s", err)
		return data, fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	if err := json.Unmarshal([]byte(d), &data); err != nil {
		log.Errorf("error while unmarshalling")
		return data, err
	}
	log.Debugf("%v\n", data)
	return data, nil
}

func (p *postgresWithIndex[T]) GetSecondary(secondary string) ([]T, error) {
	r, err := p.db.Query(fmt.Sprintf("select * from %s where secondary ='%s'", p.tableName, secondary))
	if err != nil {
		return nil, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	return getRows[T, *postgresWithIndex[T]](r, p)
}

func (p *postgresWithIndex[T]) GetPrimary(key string) ([]T, error) {
	r, err := p.db.Query(fmt.Sprintf("select * from %s where id ='%s'", p.tableName, key))
	log.Errorf("select * from %s where id ='%s'", p.tableName, key)
	if err != nil {
		return nil, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	return getRows[T, *postgresWithIndex[T]](r, p)
}

func (p *postgresWithIndex[T]) scanRow(r *sql.Rows, d *string) error {
	var id string
	var secondary string
	return r.Scan(&id, &secondary, d)
}
