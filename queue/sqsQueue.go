package queue

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"strings"
)

const HoldingElements = 10000

func (s *sqsQueue[T]) SendMessage(data T) error {
	d, err := json.Marshal(data)
	if err != nil {
		log.Errorf("could not marshall message to send, %v", err)
		return err
	}
	_, err = s.svc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(d)),
		QueueUrl:    &s.queueUrl,
	})
	return err
}

func (s *sqsQueue[T]) ReceiveMessage() (T, error) {
	var data T
	if dataReturned, err := s.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:          &s.queueUrl,
		VisibilityTimeout: aws.Int64(10),
		WaitTimeSeconds:   aws.Int64(10),
	}); err != nil {
		return data, err
	} else if len(dataReturned.Messages) > 0 {
		msg := dataReturned.Messages[0]
		err = json.Unmarshal([]byte(*msg.Body), &data)
		if err != nil {
			_, err = s.svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      &s.queueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
		}
		return data, err
	}
	return data, nil
}

func (s *sqsQueue[T]) CreateReceiver() <-chan T {
	if s.onlyReceiver == nil {
		channel := make(chan T, HoldingElements)
		s.onlyReceiver = channel
		go func() {
			for {
				if d, err := s.ReceiveMessage(); err != nil {
					log.Errorf("Error while fetching message: %v", err)
				} else if !d.IsZero() {
					s.onlyReceiver <- d
					v := <-s.sem
					for _, c := range s.multiReceiver {
						c <- d
					}
					s.sem <- v
				}
			}
		}()
	}
	return s.onlyReceiver
}

func (s *sqsQueue[T]) AddReceiver() <-chan T {
	if s.onlyReceiver == nil {
		return s.CreateReceiver()
	} else {
		v := <-s.sem
		channel := make(chan T, HoldingElements)
		s.multiReceiver = append(s.multiReceiver, channel)
		s.sem <- v
		return channel
	}
}

type sqsQueue[T CheckableZero] struct {
	onlyReceiver  chan T
	multiReceiver []chan T
	queueUrl      string
	svc           *sqs.SQS
	sem           chan struct{}
}

func CreateQueue[T CheckableZero](sess *session.Session, cfgs *aws.Config, queueName string) (Queue[T], error) {
	svc := sqs.New(sess, cfgs)
	var queueUrl string
	if queues, err := svc.ListQueues(&sqs.ListQueuesInput{}); err != nil {
		log.Errorf("error while looking for queues: %v", err)
		return nil, err
	} else {
		exists := false
		for _, q := range queues.QueueUrls {
			exists = strings.HasSuffix(*q, queueName)
			if exists {
				queueUrl = *q
				break
			}
		}
		if !exists {
			if q, err := svc.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &queueName,
				Attributes: map[string]*string{
					"MessageRetentionPeriod": aws.String("86400"),
				},
			}); err != nil {
				log.Errorf("error while creating queue: %v", err)
				return nil, err
			} else {
				queueUrl = *q.QueueUrl
			}
		}
	}
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	queue := sqsQueue[T]{
		multiReceiver: make([]chan T, 0),
		queueUrl:      queueUrl,
		svc:           svc,
		sem:           sem,
	}
	return &queue, nil
}
