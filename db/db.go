package db

import (
	"fmt"
	"log"
	"strconv"

	"github.com/boltdb/bolt"
)

type Datastore struct {
	BoltDB     *bolt.DB
	Server     string //datastore
	Logsbucket string //logs bucket
	Clients    []string
}

func CreateDb(server string, clients []string) (datastore *Datastore, closeFunc func() error, err error) {
	//create a db
	boltDB, err := bolt.Open(server, 0600, nil)
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}
	closeFunc = boltDB.Close
	db := &Datastore{BoltDB: boltDB, Server: server, Logsbucket: "logs", Clients: clients}
	boltDB.NoSync = true

	err = db.createBuckets() //Creating bucket for transactions
	if err != nil {
		log.Fatalf("Could not create dbs for server: %v", server)
		return nil, nil, err
	}

	db.Flush()

	_, err = db.PrintDB()
	if err != nil {
		log.Fatalf("Could not print balances in datastore: %v", server)
		return nil, nil, err
	}

	logs, err := db.GetAllLogs()
	if err != nil {
		log.Fatalf("Could not print balances in datastore: %v", server)
	}
	log.Println(logs)

	return db, closeFunc, nil
}

func (ds *Datastore) createBuckets() (err error) {
	err = ds.BoltDB.Update(func(tx *bolt.Tx) (err error) {
		if _, err := tx.CreateBucketIfNotExists([]byte(ds.Server)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(ds.Logsbucket)); err != nil {
			return err
		}
		return err
	})
	return err
}

func (ds *Datastore) CreateClient(bucket string, key string, value int) error {
	return ds.BoltDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Put([]byte(key), []byte(strconv.Itoa(value)))
	})
}

func (ds *Datastore) UpdateClient(key string, op string, value int) error {
	currbalance, err := ds.GetValue(key, ds.Server)
	if err != nil {
		log.Printf("unable to get curr balance for client %v", key)
		return err
	}
	intcurrbalance, err := strconv.Atoi(string(currbalance))
	if err != nil {
		log.Printf("could not covnvert balance to int for %v", key)
		return err
	}
	if op == "add" {
		return ds.BoltDB.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(ds.Server)).Put([]byte(key), []byte(strconv.Itoa(intcurrbalance+value)))
		})
	} else {
		return ds.BoltDB.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(ds.Server)).Put([]byte(key), []byte(strconv.Itoa(intcurrbalance-value)))
		})
	}
}

func (ds *Datastore) InitialiseClients() (err error) {
	for _, client := range ds.Clients {
		if err := ds.CreateClient(ds.Server, client, 10); err != nil {
			return err
		}
	}
	return nil
}

func (ds *Datastore) GetValue(key string, bucket string) ([]byte, error) {
	var res []byte
	err := ds.BoltDB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		res = bucket.Get([]byte(key))
		return nil //return of this anon func
	})
	if err == nil {
		return res, nil
	}
	return nil, err
}

func (ds *Datastore) PrintDB() ([]string, error) {
	log.Printf("Printing balances from datastore %v: ", ds.Server)
	allBalances := []string{}
	log.Println("clients=", ds.Clients)
	for _, client := range ds.Clients {
		balance, err := ds.GetValue(client, ds.Server)
		if err != nil {
			log.Println("Could not get Balance")
			return nil, err
		}
		newbalance, err := strconv.Atoi(string(balance))
		if err != nil {
			log.Println("Could not convert balance to string")
			return nil, err
		}
		bal := fmt.Sprintf("client: %v, balance:%v", client, newbalance)
		fmt.Println(bal)
		allBalances = append(allBalances, bal)
	}
	return allBalances, nil
}

func (ds *Datastore) UpdateLog(key []byte, value []byte) error {
	return ds.BoltDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(ds.Logsbucket)).Put(key, value)
	})
}
func (ds *Datastore) DeleteLog(key []byte) error {
	ds.BoltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ds.Logsbucket))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		// Delete key
		err := b.Delete([]byte(key))
		if err != nil {
			return err
		}

		return nil
	})
	return nil
}

func (ds *Datastore) GetAllLogs() (logs []string, err error) {
	err = ds.BoltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ds.Logsbucket))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			log := fmt.Sprintf("%s, log=%s", k, v)
			// fmt.Println(log)
			logs = append(logs, log)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Could not get all logs: ", err)
	}
	return logs, nil
}

func (ds *Datastore) Flush() error {
	// log.Println("Purging DB")
	err := ds.InitialiseClients()
	if err != nil {
		log.Fatalf("Could not initialise clients in datastore")
		return err
	}
	err = ds.BoltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ds.Logsbucket))
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.ForEach(func(k, v []byte) error {
			return b.Delete(k)
		})
	})

	if err != nil {
		log.Fatal(err)
	}

	return nil
}
