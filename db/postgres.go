package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

type postgresDb[T Storable] struct {
	db        *sql.DB
	tableName string
}

func (p *postgresDb[T]) Insert(obj T) {
	_, err := p.Get(obj.GetPrimaryKey())
	if err == nil {
		return
	}
	jsonObject, _ := json.Marshal(obj)
	log.Debugf("data is being saved: %v\n", obj)
	r, err := p.db.Query(fmt.Sprintf("insert into %s values ('%s', '%v')", p.tableName, obj.GetPrimaryKey(), string(jsonObject)))
	log.Debugf("rows returned %v, error %v\n", r, err)
	log.Debugf("insert into %s values (%s, '%v')", p.tableName, obj.GetPrimaryKey(), string(jsonObject))
}

func (p *postgresDb[T]) Update(obj T) {
	_, err := p.Get(obj.GetPrimaryKey())
	if err != nil {
		return
	}
	jsonObject, _ := json.Marshal(obj)
	log.Debugf("data is being saved: %v\n", obj)
	r, err := p.db.Query(fmt.Sprintf("update %s SET data='%v' where id='%s'", p.tableName, string(jsonObject), obj.GetPrimaryKey()))
	log.Debugf("rows returned %v, error %v\n", r, err)
	log.Debugf("update %s SET data='%v' where id=%d", p.tableName, string(jsonObject), obj.GetPrimaryKey())
}

func (p *postgresDb[T]) Delete(key string) T {
	r, err := p.Get(key)
	if err != nil {
		return r
	}
	p.db.Query(fmt.Sprintf("delete from %s where id = '%s'", p.tableName, key))
	return r
}

func (p *postgresDb[T]) Get(key string) (T, error) {
	var id string
	var d string
	var data T
	log.Infof("key to find is: %s", key)
	log.Infof("select * from %s where id = '%s'", p.tableName, key)
	r, err := p.db.Query(fmt.Sprintf("select * from %s where id = '%s'", p.tableName, key))
	if err != nil {
		return data, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	r.Next()
	err = r.Scan(&id, &d)
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

func CreateDB[T Storable](table string, url string) (DB[T], error) {
	db, err := sql.Open("postgres", fmt.Sprintf("%s?sslmode=disable", url))

	if err != nil {
		return nil, err
	}
	if _, err := db.Query(fmt.Sprintf("select 1 from %s", table)); err != nil {
		if _, err := db.Query(fmt.Sprintf("create table %s(id varchar(300) primary key not null, data json);", table)); err != nil {
			log.Errorf("error while creating table: %v", err)
			return nil, err
		}
	}
	return &postgresDb[T]{db: db, tableName: table}, err
}

func (p *postgresDb[T]) GetAll() ([]T, error) {
	data := make([]T, 0)
	r, err := p.db.Query(fmt.Sprintf("select * from %s", p.tableName))
	if err != nil {
		return data, fmt.Errorf("value not found: %v", err)
	}
	defer r.Close()
	return getRows[T, *postgresDb[T]](r, p)
}

func getRows[T any, S scanner](r *sql.Rows, scanner S) ([]T, error) {
	var d string
	data := make([]T, 0)
	for r.Next() {
		if err := scanner.scanRow(r, &d); err != nil {
			log.Errorf("error while getting")
			return data, fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		var iData T
		if err := json.Unmarshal([]byte(d), &iData); err != nil {
			log.Errorf("error while unmarshalling")
			return data, err
		}
		data = append(data, iData)
	}
	return data, nil
}

func (p *postgresDb[T]) scanRow(r *sql.Rows, d *string) error {
	var id string
	return r.Scan(&id, d)
}

