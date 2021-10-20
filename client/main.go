package main

import (
	"context"
	pb "github.com/test1/msg/msg"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := pb.NewServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	i := 0
	for {
		rpl, err := c.Add(ctx, &pb.Msg{Name: "Mriley" + strconv.Itoa(i)})
		if err != nil {
			log.Println("something went wrong", err)
		}
		log.Println(rpl.Name, "add")
		rpl, err = c.Add(ctx, &pb.Msg{Name: "Mriley" + strconv.Itoa(i+10)})
		if err != nil {
			log.Println("something went wrong", err)
		}
		log.Println(rpl.Name, "add")
		rply, err := c.List(ctx, &pb.Msg{Name: ""})
		if err != nil {
			log.Println("something went wrong", err)
		}
		log.Println(rply.Name, "list")
		rp, err := c.Delete(ctx, &pb.Msg{Name: "Mriley" + strconv.Itoa(i)})
		if err != nil {
			log.Println("something went wrong", err)
		}
		log.Println(rp.Name, "delete")
		i++
	}
}
