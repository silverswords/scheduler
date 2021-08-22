package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("Can't create etcd client: ", err)
	}
	watchChan := client.Watch(context.TODO(), "config", clientv3.WithPrefix())

	for event := range watchChan {
		fmt.Println(event)
	}
}
