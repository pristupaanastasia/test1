package main

import (
	"context"
	"database/sql"

	"google.golang.org/grpc/reflection"
	"strconv"
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

type server struct {
	pb.UnimplementedServiceServer
}
type user struct {
	id   int
	name string
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
			Users.Name = append(Users.Name, key)
		}
		return Users, nil
	}

	rows, err := db.Query("select * from test")
	log.Println(err)
	if err != nil {
		return &pb.SliceMsg{Name: []string{"Error"}}, err
	}

	for rows.Next() {
		p := user{}
		err = rows.Scan(&p.id, &p.name)
		if err != nil {
			return &pb.SliceMsg{Name: []string{"Error"}}, err
		}
		_, err = client.Do("SET", strconv.Itoa(p.id), p.name)
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

	lis, err := net.Listen("tcp", ":9000")
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
