package main

import (
	"context"
	pb "github.com/test1/msg/msg"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := pb.NewServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rpl, err := c.Add(ctx, &pb.Msg{Name: "Mriley"})
	if err != nil {
		log.Println("something went wrong", err)
	}
	log.Println(rpl.Name)
	rply, err := c.List(ctx, &pb.Msg{Name: "Mriley"})
	if err != nil {
		log.Println("something went wrong", err)
	}
	log.Println(rply.Name)
	rp, err := c.Delete(ctx, &pb.Msg{Name: "Mriley"})
	if err != nil {
		log.Println("something went wrong", err)
	}
	log.Println(rp.Name)
}
