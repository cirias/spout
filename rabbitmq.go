package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

var RabbitmqURL = "amqp://guest:guest@localhost:5672/"

func SendItem(item interface{}) error {
	conn, err := amqp.Dial(RabbitmqURL)
	if err != nil {
		return errors.Wrap(err, "could not dial amqp")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrap(err, "could not get channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"items", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return errors.Wrap(err, "could not declare queue")
	}

	body, err := json.Marshal(item)
	if err != nil {
		return errors.Wrap(err, "could not marshal item")
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	return errors.Wrap(err, "could not publish item")
}

func GetItems() (<-chan Item, error) {
	conn, err := amqp.Dial(RabbitmqURL)
	if err != nil {
		return nil, errors.Wrap(err, "could not dial amqp")
	}
	// defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "could not get channel")
	}
	// defer ch.Close()

	q, err := ch.QueueDeclare(
		"items", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not declare queue")
	}

	out := make(chan Item)
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not consume")
	}

	go func() {
		log.Println("reading msgs")
		for msg := range msgs {
			go handle(out, msg)
		}
		log.Println("reading msgs quit")
	}()

	return out, nil
}

func handle(out chan<- Item, msg amqp.Delivery) {
	item, err := decodeItem(msg.Body)
	if err != nil {
		log.Println("could not decode item:", err)
		return
	}

	out <- item
}

func decodeItem(bs []byte) (Item, error) {
	log.Println(string(bs))
	m := &struct {
		Type string `json:"type"`
	}{}
	if err := json.Unmarshal(bs, m); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal message")
	}

	for _, dag := range dags {
		for _, resource := range dag.Resources() {
			if resource.Name() != m.Type {
				continue
			}

			item := resource.NewItem()
			if err := json.Unmarshal(bs, item); err != nil {
				return nil, errors.Wrap(err, "could not unmarshal item")
			}

			return item, nil
		}
	}

	return nil, fmt.Errorf("unknown resource type: %s", m.Type)
}

func SendJob(job interface{}) error {
	conn, err := amqp.Dial(RabbitmqURL)
	if err != nil {
		return errors.Wrap(err, "could not dial amqp")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrap(err, "could not get channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"jobs", // name
		false,  // durable
		false,  // delete when usused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		return errors.Wrap(err, "could not declare queue")
	}

	body, err := json.Marshal(job)
	if err != nil {
		return errors.Wrap(err, "could not marshal job")
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	return errors.Wrap(err, "could not publish job")
}
