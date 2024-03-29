package txn

import (
	"context"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"sync/atomic"
)

type timeMessage struct {
	timestamp uint64
	done      bool
}

type callbackMessage struct {
	timestamp       uint64
	outNotification chan struct{}
}

// TransactionTimestampMark TODO: DO we need doneTill to be atomic?
type TransactionTimestampMark struct {
	doneTill                              atomic.Uint64
	timestampChannel                      chan timeMessage
	callBackChannel                       chan callbackMessage
	stopChannel                           chan struct{}
	pendingTransactionRequestsByTimestamp *treemap.Map
	notificationChannelsByTimestamp       map[uint64][]chan struct{}
}

func NewTransactionTimestampMark() *TransactionTimestampMark {
	transactionMark := &TransactionTimestampMark{
		timestampChannel:                      make(chan timeMessage),
		callBackChannel:                       make(chan callbackMessage),
		stopChannel:                           make(chan struct{}),
		pendingTransactionRequestsByTimestamp: treemap.NewWith(utils.UInt64Comparator),
		notificationChannelsByTimestamp:       make(map[uint64][]chan struct{}),
	}
	go transactionMark.spin()
	return transactionMark
}

func (this *TransactionTimestampMark) Begin(timestamp uint64) {
	this.timestampChannel <- timeMessage{timestamp: timestamp, done: false}
}

func (this *TransactionTimestampMark) Finish(timestamp uint64) {
	this.timestampChannel <- timeMessage{timestamp: timestamp, done: true}
}

func (this *TransactionTimestampMark) Stop() {
	this.stopChannel <- struct{}{}
}

func (this *TransactionTimestampMark) DoneTill() uint64 {
	return this.doneTill.Load()
}

func (this *TransactionTimestampMark) WaitForMark(
	ctx context.Context,
	timestamp uint64,
) error {
	if this.DoneTill() >= timestamp {
		return nil
	}
	waitChannel := make(chan struct{})
	this.callBackChannel <- callbackMessage{timestamp: timestamp, outNotification: waitChannel}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChannel:
		return nil
	}
}

func (this *TransactionTimestampMark) process(message timeMessage) {
	//fmt.Printf("Received message %d, %t \n", message.timestamp, message.done)
	previous, ok := this.pendingTransactionRequestsByTimestamp.Get(message.timestamp)
	updated := 0
	if ok {
		updated = previous.(int)
	}
	if message.done {
		updated--
	} else {
		updated++
	}
	//fmt.Printf("Updating map with ts = %d with value = %d\n", message.timestamp, updated)
	this.pendingTransactionRequestsByTimestamp.Put(message.timestamp, updated)

	doneTill := this.DoneTill()
	localDoneTillTimestamp := doneTill

	keysToRemove := make([]uint64, 0)
	it := this.pendingTransactionRequestsByTimestamp.Iterator()
	//TODO: Check if we are missing the first element over here
	//fmt.Println("Map size right now = ", this.pendingTransactionRequestsByTimestamp.Size())
	for it.Next() {
		key, value := it.Key().(uint64), it.Value().(int)
		//fmt.Printf("Checking pair: %d, %d\n", key, value)
		if value > 0 {
			//fmt.Println("Breaking here")
			break
		}
		keysToRemove = append(keysToRemove, key)
		//fmt.Printf("Updating localDoneTillTimestamp to: %d\n", key)
		localDoneTillTimestamp = key
	}

	for _, key := range keysToRemove {
		this.pendingTransactionRequestsByTimestamp.Remove(key)
	}

	if localDoneTillTimestamp != doneTill {
		this.doneTill.CompareAndSwap(doneTill, localDoneTillTimestamp)
	}

	for timestamp, notificationChannels := range this.notificationChannelsByTimestamp {
		if timestamp <= localDoneTillTimestamp {
			for _, channel := range notificationChannels {
				close(channel)
			}
			delete(this.notificationChannelsByTimestamp, timestamp)
		}
	}
}

func (this *TransactionTimestampMark) spin() {
	for {
		select {
		case notification := <-this.callBackChannel:
			doneTill := this.doneTill.Load()
			if doneTill >= notification.timestamp {
				close(notification.outNotification)
			} else {
				channels, ok := this.notificationChannelsByTimestamp[notification.timestamp]
				if !ok {
					this.notificationChannelsByTimestamp[notification.timestamp] = []chan struct{}{notification.outNotification}
				} else {
					this.notificationChannelsByTimestamp[notification.timestamp] = append(channels, notification.outNotification)
				}
			}
		case tsMessage := <-this.timestampChannel:
			this.process(tsMessage)
		case <-this.stopChannel:
			close(this.timestampChannel)
			close(this.callBackChannel)
			close(this.stopChannel)
			closeAll(this.notificationChannelsByTimestamp)
			return
		}
	}

}

func closeAll(notificationChannelsByTimestamp map[uint64][]chan struct{}) {
	for timestamp, notificationChannels := range notificationChannelsByTimestamp {
		for _, channel := range notificationChannels {
			close(channel)
		}
		delete(notificationChannelsByTimestamp, timestamp)
	}
}
