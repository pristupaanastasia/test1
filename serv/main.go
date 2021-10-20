package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/reflection"
	"time"

	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
	pb "github.com/test1/msg/msg"
	"google.golang.org/grpc"
	"log"
	"net"
)

var db *sql.DB
var pool = newPool()
var client redis.Conn
var stm *sql.Stmt

type server struct {
	pb.UnimplementedServiceServer
}
type user struct {
	id   int
	name string
}

func readKafka() {
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	for {
		m, err := batch.ReadMessage()
		if err != nil {
			continue
		}
		_, err = stm.Exec(m.Value, m.Time)
		if err != nil {
			log.Println(err)
		}
	}

}
func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     80,
		MaxActive:   12000,
		IdleTimeout: time.Minute,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}
func (s *server) Add(ctx context.Context, in *pb.Msg) (*pb.Msg, error) {

	log.Println(in.Name)

	result, err := db.Exec("insert into test (name) values ($1)", in.Name)
	if err != nil {
		log.Println(err)
	}

	topic := "my-topic"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte(in.Name)},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	if err != nil {
		return &pb.Msg{Name: "Error"}, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return &pb.Msg{Name: "Error RowsAffected"}, err
	}
	return &pb.Msg{Name: "Ok"}, nil
}
func (s *server) Delete(ctx context.Context, in *pb.Msg) (*pb.Msg, error) {
	result, err := db.Exec("delete from test where name = $1", in.Name)
	if err != nil {
		return &pb.Msg{Name: "Error"}, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return &pb.Msg{Name: "Error RowsAffected"}, err
	}
	return &pb.Msg{Name: "Ok"}, nil
}
func (s *server) List(ctx context.Context, in *pb.Msg) (*pb.SliceMsg, error) {
	Users := &pb.SliceMsg{}
	keys, err := redis.Strings(client.Do("KEYS", "*"))
	if err == nil && len(keys) > 0 {
		for _, key := range keys {
			log.Println(key)
			Users.Name = append(Users.Name, key)
		}
		return Users, nil
	}

	rows, err := db.Query("select * from test")
	if err != nil {
		return &pb.SliceMsg{Name: []string{"Error"}}, err
	}

	for rows.Next() {
		p := user{}
		err = rows.Scan(&p.id, &p.name)
		if err != nil {
			return &pb.SliceMsg{Name: []string{"Error"}}, err
		}
		_, err = client.Do("SET", p.name, p.id)
		client.Do("EXPIRE", p.name, time.Minute)
		if err != nil {
			log.Println(err)
		}
		Users.Name = append(Users.Name, p.name)
	}
	return Users, nil
}
func main() {
	var err error
	connStr := "user=mriley password=root dbname=postgres sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&username=default&password=1805")
	if err != nil {
		log.Fatal(err)
	}
	if err = connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS test (
			name_user String(50),
			action_time  DateTime
		) engine=Memory
	`)
	if err != nil {
		log.Fatal(err)
	}

	tx, _ := connect.Begin()
	stm, _ = tx.Prepare("INSERT INTO test (name_user,action_time) VALUES (?, ?)")
	defer stm.Close()

	go readKafka()
	lis, err := net.Listen("tcp", ":9080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	client = pool.Get()
	defer client.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterServiceServer(grpcServer, &server{})
	reflection.Register(grpcServer)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatal("failed to serve", err)
	}

}
